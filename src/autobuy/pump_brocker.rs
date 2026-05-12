use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use sips::instructions::{
    pump::instructions::PumpInstruction, token_program_2022::TokenProgram2022,
};
use solana_address::Address as SolAddress;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_keypair::Keypair;
// Use the modular solana crate instead of the monolithic solana_sdk

use crate::generalize::{general_commands::TradeAction, general_pool::Pool};

use super::broker::{Broker, BrokerError, BuyReceipt, SellReceipt};

// ── State per open position ───────────────────────────────────────────────────

pub struct Position {
    pub tokens: f64,
    pub entry_mcap: f64,
}

// ── Solana Broker ─────────────────────────────────────────────────────────────

pub struct SolanaBroker {
    /// The RPC Client used to fetch on-chain data and send transactions.
    rpc_client: Arc<RpcClient>,

    keypair: Arc<Keypair>,
    /// The address of the autobuy wallet.
    wallet_address: SolAddress,

    // Internal state tracked via the live trade stream.
    balance: Mutex<f64>,
    positions: Mutex<HashMap<SolAddress, Position>>,
}

impl SolanaBroker {
    /// Initializes the broker and fetches the actual SOL balance from the blockchain.
    pub async fn new(
        rpc_url: String,
        wallet_address: SolAddress,
        keypair: Arc<Keypair>,
    ) -> Result<Self, BrokerError> {
        let rpc_client = Arc::new(RpcClient::new(rpc_url));

        // Fetch initialized balance from chain in lamports and convert to SOL.
        let pubkey_str = wallet_address.to_string();
        let pubkey = pubkey_str
            .parse()
            .map_err(|_| BrokerError::Custom("Invalid Address".into()))?;

        let lamports = rpc_client
            .get_balance(&pubkey)
            .await
            .map_err(|e| BrokerError::Custom(e.to_string()))?;

        let initial_balance_sol = lamports as f64 / 1_000_000_000.0;

        println!(
            "[BROKER INIT] Starting SOL Balance: {:.6}",
            initial_balance_sol
        );

        Ok(Self {
            rpc_client,
            keypair,
            wallet_address,
            balance: Mutex::new(initial_balance_sol),
            positions: Mutex::new(HashMap::new()),
        })
    }

    /// This method should be called inside your event loop for every incoming `TradeAction`.
    /// It checks if the trade belongs to this broker's autobuy wallet and updates the state.
    pub fn process_trade_event(&self, trade: &TradeAction, pool: &dyn Pool) {
        // Only process trades where the trader matches our autobuy wallet.
        if trade.trader() != self.wallet_address {
            return;
        }

        let mut balance = self.balance.lock().unwrap();
        let mut positions = self.positions.lock().unwrap();
        let mint = trade.mint();

        match trade {
            TradeAction::Buy(buy) => {
                // Deduct spent amount from our balance
                let spent_sol = buy.spent.amount().to_float();
                *balance -= spent_sol;

                let entry_mcap = pool.market_cap().amount().to_float();
                let tokens_received = buy.bought.to_float();

                // Insert or add to existing position
                let pos = positions.entry(mint).or_insert(Position {
                    tokens: 0.0,
                    entry_mcap,
                });
                pos.tokens += tokens_received;
            }
            TradeAction::Sell(sell) => {
                // Add received amount back to our balance
                let received_sol = sell.received.amount().to_float();
                *balance += received_sol;

                let tokens_sold = sell.sold.to_float();

                // Reduce position size and remove if completely sold
                if let Some(pos) = positions.get_mut(&mint) {
                    pos.tokens -= tokens_sold;

                    if pos.tokens <= 0.0 {
                        positions.remove(&mint);
                        println!("[BROKER DEBUG] STATUS : Position fully CLOSED for this mint.");
                    } else {
                        println!(
                            "[BROKER DEBUG] Holding: {:.2} TOKENS remaining.",
                            pos.tokens
                        );
                    }
                } else {
                    println!(
                        "[BROKER DEBUG] WARNING: Sold tokens for a mint not tracked in local positions!"
                    );
                }
            }
        }
        println!("============================================================");
    }
}

#[async_trait]
impl Broker for SolanaBroker {
    async fn buy(
        &self,
        mint: SolAddress,
        amount_sol: f64,
        pool: &dyn Pool,
    ) -> Result<BuyReceipt, BrokerError> {
        // Sanity check before broadcasting transaction
        let bal = *self.balance.lock().unwrap();
        if bal < amount_sol {
            return Err(BrokerError::InsufficientBalance {
                have: bal,
                need: amount_sol,
            });
        }

        // 1. Convert amounts using your SDK wrapper
        let sol_amount_in = sips::helper::Amount::<9>::from_float(amount_sol);
        let min_token_out = sips::helper::Amount::<6>::from_float(1.0);

        // 2. Build the exact instruction using the provided PumpFun SDK
        let ix = PumpInstruction::buy_exact_in(
            mint.into(),
            self.wallet_address.into(),
            pool.creators()[0].into(), // pool.creator_address(),
            TokenProgram2022::PROGRAM,         // spl_token::id() mapped to your Address type
            sol_amount_in,
            min_token_out,
        );

        // 3. Attach instruction to a transaction, sign, and send via `self.rpc_client`
        let pubkey_str = self.wallet_address.to_string();
        let payer_pubkey = pubkey_str
            .parse()
            .map_err(|_| BrokerError::Custom("Invalid Payer Address".into()))?;

        let recent_blockhash = self
            .rpc_client
            .get_latest_blockhash()
            .await
            .map_err(|e| BrokerError::Custom(e.to_string()))?;

        let tx = solana_client::rpc_response::transaction::Transaction::new_signed_with_payer(
            &[ix.into()],
            Some(&payer_pubkey),
            &[&*self.keypair],
            recent_blockhash,
        );

        let sig = self
            .rpc_client
            .send_transaction(&tx)
            .await
            .map_err(|e| BrokerError::Custom(format!("Buy Tx Failed: {}", e)))?;

        println!("[BROKER TX] BUY Transaction Sent. Signature: {}", sig);

        // Return an optimistic or empty receipt (since final amounts are resolved in the loop)
        Ok(BuyReceipt {
            sol_spent: amount_sol,
            tokens_received: 0.0, // This is expected to be 0.0 here, resolved later by the event loop
        })
    }

    async fn sell(
        &self,
        mint: SolAddress,
        token_amount: f64,
        pool: &dyn Pool,
    ) -> Result<SellReceipt, BrokerError> {
        // 1. Resolve actual token amount dynamically
        let actual_token_amount = {
            let positions = self.positions.lock().unwrap();
            let pos = positions
                .get(&mint)
                .ok_or(BrokerError::PositionNotFound(mint))?;

            // If the manager passed 0.0 (because of our optimistic BuyReceipt),
            // use the actual balance we observed from the websocket stream!
            if token_amount <= 0.0 {
                println!(
                    "[BROKER DEBUG] Manager requested 0.0 sell. Auto-injecting tracked balance: {:.2}",
                    pos.tokens
                );
                pos.tokens
            } else {
                token_amount
            }
        };

        // Safety check to ensure we aren't STILL trying to sell 0
        if actual_token_amount <= 0.0 {
            return Err(BrokerError::Custom(
                "Calculated token amount is 0. Position might not be updated via WS yet.".into(),
            ));
        }

        println!(
            "[BROKER DEBUG] Preparing to sell {:.2} tokens of {}",
            actual_token_amount, mint
        );

        // 2. Build amounts
        let token_amount_in = sips::helper::Amount::<6>::from_float(actual_token_amount);

        // 🚨 IMPORTANT SLIPPAGE FIX:
        // A min_sol_out of 1.0 means the trade WILL FAIL if you receive anything less than 1 full SOL.
        // I changed it to 0.0 to prevent slippage errors. You should calculate real slippage here later.
        let min_sol_out = sips::helper::Amount::<9>::from_float(0.0);

        // 3. Build Instruction
        let ix = PumpInstruction::sell(
            mint.into(),
            self.wallet_address.into(),
            pool.creators()[0].into(), // pool.creator_address(),
            TokenProgram2022::PROGRAM,         // spl_token::id(),
            token_amount_in,
            min_sol_out,
        );

        // 4. Build transaction, sign, and send via `self.rpc_client`
        let pubkey_str = self.wallet_address.to_string();
        let payer_pubkey = pubkey_str
            .parse()
            .map_err(|_| BrokerError::Custom("Invalid Payer Address".into()))?;

        let recent_blockhash = self
            .rpc_client
            .get_latest_blockhash()
            .await
            .map_err(|e| BrokerError::Custom(e.to_string()))?;

        let tx = solana_client::rpc_response::transaction::Transaction::new_signed_with_payer(
            &[ix.into()],
            Some(&payer_pubkey),
            &[&*self.keypair],
            recent_blockhash,
        );

        let sig = self
            .rpc_client
            .send_transaction(&tx)
            .await
            .map_err(|e| BrokerError::Custom(format!("Sell Tx Failed: {}", e)))?;

        println!("[BROKER TX] SELL Transaction Sent. Signature: {}", sig);

        // Note: Do NOT remove balance/tokens here. Wait for `process_trade_event`.

        Ok(SellReceipt { sol_received: 0.0 })
    }

    async fn balance_sol(&self) -> Result<f64, BrokerError> {
        Ok(*self.balance.lock().unwrap())
    }
}
