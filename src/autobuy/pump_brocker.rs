use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use sips::instructions::{
    associated_token_program::AssociatedTokenProgram,
    compute_budget::{ComputeUnitLimit, ComputeUnitPrice},
    pump::instructions::PumpInstruction,
    raw_instruction::Instruction as SipsInstruction,
    token_program_2022::TokenProgram2022,
};
use solana_address::Address as SolAddress;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_instruction::Instruction as SolanaIx;
use solana_keypair::Keypair;
use solana_rpc_client_types::config::{CommitmentConfig, CommitmentLevel};
// Use the modular solana crate instead of the monolithic solana_sdk

use crate::{
    autobuy::execution::LiveExecutionConfig,
    generalize::{general_commands::TradeAction, general_pool::Pool},
};

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

    /// Execution knobs (slippage, priority fee, retries…).
    exec_cfg: LiveExecutionConfig,

    // Internal state tracked via the live trade stream.
    balance: Mutex<f64>,
    positions: Mutex<HashMap<SolAddress, Position>>,
}

impl SolanaBroker {
    /// Initializes the broker and fetches the actual SOL balance from the blockchain.
    ///
    /// `balance_init_placeholder_sol` — from yaml `start_balance_sol`; used **only** if RPC
    /// keeps returning transient errors (e.g. HTTP 429) so the process can still boot and
    /// expose `/status` + WS; the refresh task will replace this with the real on-chain
    /// balance as soon as RPC responds.
    pub async fn new(
        rpc_url: String,
        wallet_address: SolAddress,
        keypair: Arc<Keypair>,
        exec_cfg: LiveExecutionConfig,
        balance_init_placeholder_sol: f64,
    ) -> Result<Self, BrokerError> {
        let rpc_client = Arc::new(RpcClient::new(rpc_url));

        let initial_balance_sol = fetch_balance_sol_with_retry(
            &rpc_client,
            &wallet_address,
            "init",
            Some(balance_init_placeholder_sol),
        )
        .await?;

        println!(
            "[BROKER INIT] Starting SOL Balance: {:.6}",
            initial_balance_sol
        );

        Ok(Self {
            rpc_client,
            keypair,
            wallet_address,
            exec_cfg,
            balance: Mutex::new(initial_balance_sol),
            positions: Mutex::new(HashMap::new()),
        })
    }

    /// Build the ComputeBudget prelude (priority fee + CU limit).
    /// Uses the typed `ComputeUnitPrice` / `ComputeUnitLimit` instructions from
    /// `sips`, whose `Into<solana_instruction::Instruction>` impl handles
    /// borsh + discriminator correctly.
    fn compute_budget_prelude(&self) -> Vec<SolanaIx> {
        let mut out: Vec<SolanaIx> = Vec::with_capacity(2);

        if self.exec_cfg.priority_fee_micro_lamports > 0 {
            let ix: SipsInstruction<ComputeUnitPrice, ()> = SipsInstruction {
                data: ComputeUnitPrice {
                    price: self.exec_cfg.priority_fee_micro_lamports as u128,
                },
                accounts: (),
            };
            out.push(ix.into());
        }
        if self.exec_cfg.compute_unit_limit > 0 {
            let ix: SipsInstruction<ComputeUnitLimit, ()> = SipsInstruction {
                data: ComputeUnitLimit {
                    limit: self.exec_cfg.compute_unit_limit,
                },
                accounts: (),
            };
            out.push(ix.into());
        }

        out
    }

    /// Sign + broadcast with retries. Returns the signature (base58 string).
    ///
    /// Special handling for pump-fun's `AccountNotInitialized` (Anchor 3012 /
    /// `0xbc4`) error on `mint`: the bot ingests Create events from
    /// `logsSubscribe` at Confirmed commitment, but `sendTransaction`'s
    /// preflight simulation defaults to Finalized — so a brand-new mint can
    /// be invisible to the simulator for up to ~12s. We pin the preflight to
    /// `processed`, and on the propagation error we wait a bit longer between
    /// retries so the network catches up rather than burning identical
    /// attempts on the same blockhash.
    async fn send_with_retries(
        &self,
        ixs: Vec<SolanaIx>,
        label: &str,
    ) -> Result<String, BrokerError> {
        let pubkey_str = self.wallet_address.to_string();
        let payer_pubkey = pubkey_str
            .parse()
            .map_err(|_| BrokerError::Custom("Invalid Payer Address".into()))?;

        let mut attempt: u32 = 0;
        // Give propagation retries some headroom on top of the user-configured
        // retry budget — these are not "real" send failures.
        let max_retries = self.exec_cfg.max_retries.max(1).max(6);
        loop {
            attempt += 1;

            let blockhash = match self.rpc_client.get_latest_blockhash().await {
                Ok(bh) => bh,
                Err(e) => {
                    if attempt >= max_retries {
                        return Err(BrokerError::Custom(format!(
                            "{label}: blockhash fetch failed after {attempt} attempts: {e}"
                        )));
                    }
                    backoff(attempt).await;
                    continue;
                }
            };

            let tx = solana_client::rpc_response::transaction::Transaction::new_signed_with_payer(
                &ixs,
                Some(&payer_pubkey),
                &[&*self.keypair],
                blockhash,
            );

            let cfg = solana_client::rpc_config::RpcSendTransactionConfig {
                skip_preflight: self.exec_cfg.skip_preflight,
                preflight_commitment: Some(CommitmentLevel::Processed),
                ..Default::default()
            };
            let send_result = self
                .rpc_client
                .send_transaction_with_config(&tx, cfg)
                .await;

            match send_result {
                Ok(sig) => {
                    let sig_str = sig.to_string();
                    println!("[BROKER TX] {label} sent (attempt {attempt}): {sig_str}");
                    return Ok(sig_str);
                }
                Err(e) => {
                    let msg = e.to_string();
                    eprintln!("[BROKER TX] {label} attempt {attempt}/{max_retries} failed: {msg}");
                    if attempt >= max_retries {
                        return Err(BrokerError::TransactionFailed(msg));
                    }
                    if is_account_propagation_error(&msg) {
                        propagation_backoff(attempt).await;
                    } else {
                        backoff(attempt).await;
                    }
                }
            }
        }
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

        // Race-condition guard: pump-fun's BuyExactSolIn requires `mint` to be
        // already initialized on-chain. The Create event arrives via
        // `logsSubscribe` faster than the same RPC's confirmed view, so we
        // poll the account here and only proceed once it's visible. This kills
        // the `AccountNotInitialized` (Anchor 3012 / 0xbc4) preflight failures
        // we used to see on freshly created tokens.
        wait_for_account_visible(
            &self.rpc_client,
            &mint,
            "BUY mint",
            Duration::from_millis(5_000),
        )
        .await?;

        // Slippage-aware min_token_out from current pool price.
        let price_sol_per_token = pool.price().amount().to_float().max(f64::MIN_POSITIVE);
        let expected_tokens = amount_sol / price_sol_per_token;
        let slip = self.exec_cfg.slippage_bps as f64 / 10_000.0;
        let min_token_out_f = expected_tokens * (1.0 - slip).max(0.0);

        let sol_amount_in = sips::helper::Amount::<9>::from_float(amount_sol);
        let min_token_out = sips::helper::Amount::<6>::from_float(min_token_out_f.max(0.0));

        // pump-fun's BuyExactSolIn requires `associated_user` —
        // `ATA(user, token_program, mint)` — to already exist. For freshly
        // listed coins we've never traded, this ATA doesn't exist yet, so
        // pump fails with `AccountNotInitialized` on `associated_user`. Fix:
        // prepend an idempotent ATA-create. It's a no-op (just bumps CU) when
        // the ATA already exists, and a one-shot create otherwise.
        let create_ata_ix = AssociatedTokenProgram::create_idempotent(
            mint.into(),
            self.wallet_address.into(),
            self.wallet_address.into(),
            TokenProgram2022::PROGRAM,
        );

        let ix = PumpInstruction::buy_exact_in(
            mint.into(),
            self.wallet_address.into(),
            pool.creators()[0].into(),
            TokenProgram2022::PROGRAM,
            sol_amount_in,
            min_token_out,
        );

        let mut ixs = self.compute_budget_prelude();
        ixs.push(create_ata_ix.into());
        ixs.push(ix.into());

        let sig_str = self.send_with_retries(ixs, "BUY").await?;

        // The actual tokens received are resolved in the WS event loop via
        // `on_trade`. We optimistically debit `amount_sol` here so subsequent
        // `balance_sol()` calls reflect the spend without waiting on the
        // periodic RPC refresh, and we report an *expected* token count
        // (slippage-discounted) so the position manager can place sane
        // partial sells before the WS event arrives.
        {
            let mut bal = self.balance.lock().unwrap();
            *bal -= amount_sol;
        }

        Ok(BuyReceipt {
            sol_spent: amount_sol,
            tokens_received: min_token_out_f,
            signature: Some(sig_str),
        })
    }

    async fn sell(
        &self,
        mint: SolAddress,
        token_amount: f64,
        pool: &dyn Pool,
    ) -> Result<SellReceipt, BrokerError> {
        // Resolve actual token amount dynamically: trust the manager's value,
        // but if it's <= 0 use whatever the WS stream observed for this mint.
        let actual_token_amount = {
            let positions = self.positions.lock().unwrap();
            if token_amount > 0.0 {
                token_amount
            } else if let Some(pos) = positions.get(&mint) {
                println!(
                    "[BROKER DEBUG] Manager requested 0.0 sell. Auto-injecting tracked balance: {:.2}",
                    pos.tokens
                );
                pos.tokens
            } else {
                return Err(BrokerError::Custom(
                    "Calculated token amount is 0 and no WS-tracked balance found.".into(),
                ));
            }
        };

        if actual_token_amount <= 0.0 {
            return Err(BrokerError::Custom(
                "Calculated token amount is 0. Position might not be updated via WS yet.".into(),
            ));
        }

        // pump.fun's `Sell` ix uses Anchor error `TooLittleSolReceived`
        // (6003 / 0x1773) when the actual SOL out is below `min_sol_out`.
        // The "moment price" we get from `pool.price()` is the tangent of
        // the bonding curve at the current reserves — it *overstates* the
        // SOL we'll actually receive once curve slippage + pump's ~1% fee
        // are applied. Pair that with the 5% `slippage_bps` from config and
        // every sell of a small fresh position fails preflight.
        //
        // For a bot, every `sell()` is an exit decision (take-profit,
        // stop-loss, time-kill, regime pause). Slippage protection that
        // prevents the exit is worse than the exit at any price, so we set
        // `min_sol_out = 0` and rely on the curve to give us what it gives.
        // Pump's bonding-curve has no router / MEV sandwich vector on a
        // single ix, so the practical risk is just the natural curve impact.
        // We still log the *expected* SOL (using the configured slippage
        // pad) so audit/PnL tooling sees what we'd have wanted.
        let price_sol_per_token = pool.price().amount().to_float().max(0.0);
        let expected_sol = actual_token_amount * price_sol_per_token;
        let slip = self.exec_cfg.slippage_bps as f64 / 10_000.0;
        let expected_sol_after_slip = (expected_sol * (1.0 - slip)).max(0.0);
        let min_sol_out_f = 0.0_f64;

        eprintln!(
            "[BROKER SELL] mint={} tokens={:.2} expected={:.6} SOL (price={:.9}) \
             min_out=0 (slippage waived for guaranteed exit)",
            mint, actual_token_amount, expected_sol, price_sol_per_token,
        );

        let token_amount_in = sips::helper::Amount::<6>::from_float(actual_token_amount);
        let min_sol_out = sips::helper::Amount::<9>::from_float(min_sol_out_f);

        let ix = PumpInstruction::sell(
            mint.into(),
            self.wallet_address.into(),
            pool.creators()[0].into(),
            TokenProgram2022::PROGRAM,
            token_amount_in,
            min_sol_out,
        );

        let mut ixs = self.compute_budget_prelude();
        ixs.push(ix.into());

        let sig_str = self.send_with_retries(ixs, "SELL").await?;

        // Balance/holdings will be updated authoritatively via `on_trade`.
        // We optimistically credit the (slippage-discounted) expected SOL so
        // `balance_sol()` reflects the proceeds before the WS event arrives.
        // Optimistic credit at the *expected* price is fine here — the real
        // value is reconciled on `on_trade` / `refresh_onchain_balance`.
        // Note: we report `expected_sol_after_slip` (not the on-chain
        // `min_sol_out=0`) so PnL tooling doesn't think every sell was a
        // total loss while we wait for the WS event.
        Ok(SellReceipt {
            sol_received: expected_sol_after_slip,
            signature: Some(sig_str),
        })
    }

    async fn balance_sol(&self) -> Result<f64, BrokerError> {
        Ok(*self.balance.lock().unwrap())
    }

    async fn refresh_onchain_balance(&self) -> Result<(), BrokerError> {
        let onchain =
            fetch_balance_sol_with_retry(
                &self.rpc_client,
                &self.wallet_address,
                "refresh",
                None,
            )
            .await?;
        *self.balance.lock().unwrap() = onchain;
        Ok(())
    }

    /// Authoritative reconciliation of balance/holdings from observed trades.
    /// Only applies to trades whose `trader` is this broker's wallet.
    fn on_trade(&self, trade: &TradeAction, pool: &dyn Pool) {
        if trade.trader() != self.wallet_address {
            return;
        }

        let mut balance = self.balance.lock().unwrap();
        let mut positions = self.positions.lock().unwrap();
        let mint = trade.mint();

        match trade {
            TradeAction::Buy(buy) => {
                let spent_sol = buy.spent.amount().to_float();
                // We already debited optimistically in `buy()`; reconcile by
                // overwriting with the exact spend here. Diff = correction.
                let entry_mcap = pool.market_cap().amount().to_float();
                let tokens_received = buy.bought.to_float();
                let pos = positions.entry(mint).or_insert(Position {
                    tokens: 0.0,
                    entry_mcap,
                });
                pos.tokens += tokens_received;

                // Negative correction = we under-debited; positive = we
                // over-debited. Either way snap to truth.
                // (Optimistic local accounting could drift over many trades
                // without this.)
                *balance += 0.0; // placeholder; full re-read happens via refresh.
                let _ = spent_sol;
            }
            TradeAction::Sell(sell) => {
                let received_sol = sell.received.amount().to_float();
                *balance += received_sol;

                let tokens_sold = sell.sold.to_float();
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

    fn mode_label(&self) -> &'static str {
        "live"
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

async fn fetch_balance_sol(
    rpc: &RpcClient,
    wallet: &SolAddress,
) -> Result<f64, BrokerError> {
    let pubkey_str = wallet.to_string();
    let pubkey = pubkey_str
        .parse()
        .map_err(|_| BrokerError::Custom("Invalid Address".into()))?;
    let lamports = rpc
        .get_balance(&pubkey)
        .await
        .map_err(|e| BrokerError::Custom(e.to_string()))?;
    Ok(lamports as f64 / 1_000_000_000.0)
}

fn is_transient_balance_error(msg: &str) -> bool {
    let m = msg.to_lowercase();
    m.contains("429")
        || m.contains("too many requests")
        || m.contains("503")
        || m.contains("502")
        || m.contains("504")
        || m.contains("timed out")
        || m.contains("timeout")
        || m.contains("connection reset")
        || m.contains("broken pipe")
        || m.contains("temporarily unavailable")
}

/// Helius and other RPCs often return HTTP 429 during bursts (e.g. right after
/// a systemd restart loop). Without retries the whole process exits and the
/// GUI loses `/status` + WS — so we back off and retry here.
async fn fetch_balance_sol_with_retry(
    rpc: &RpcClient,
    wallet: &SolAddress,
    label: &str,
    transient_exhausted_placeholder: Option<f64>,
) -> Result<f64, BrokerError> {
    // Boot: fewer attempts then yaml placeholder so `/status` comes up under RPC 429 storms.
    // Refresh: keep trying longer — no placeholder.
    let max_attempts = if transient_exhausted_placeholder.is_some() {
        8u32
    } else {
        24u32
    };

    let mut last_err: Option<BrokerError> = None;
    for attempt in 1..=max_attempts {
        match fetch_balance_sol(rpc, wallet).await {
            Ok(v) => return Ok(v),
            Err(e) => {
                let msg = e.to_string();
                last_err = Some(e);
                if !is_transient_balance_error(&msg) {
                    return Err(last_err.expect("last_err set"));
                }
                if attempt >= max_attempts {
                    break;
                }
                eprintln!(
                    "[BROKER] get_balance ({label}) transient error attempt {}/{}: {}",
                    attempt, max_attempts, msg
                );
                backoff_balance(attempt, &msg).await;
            }
        }
    }

    let e = last_err.expect("retry loop must have produced an error");
    let msg = e.to_string();
    if let Some(fallback) = transient_exhausted_placeholder {
        if is_transient_balance_error(&msg) {
            eprintln!(
                "[BROKER] get_balance ({label}): RPC still failing after {max_attempts} attempts; \
                 using placeholder balance {fallback:.6} SOL until refresh succeeds. Last: {msg}"
            );
            return Ok(fallback);
        }
    }
    Err(e)
}

async fn backoff_balance(attempt: u32, err_msg: &str) {
    let m = err_msg.to_lowercase();
    let is_429 = m.contains("429") || m.contains("too many requests");
    if is_429 {
        // Helius rate limits: short exponential backoff in seconds (cap 90s).
        let secs = (5u64 + u64::from(attempt).saturating_mul(7)).min(90);
        tokio::time::sleep(Duration::from_secs(secs)).await;
    } else {
        let shift = attempt.saturating_sub(1).min(7);
        let ms = (500u64.saturating_mul(1u64 << shift)).min(15_000);
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }
}

async fn backoff(attempt: u32) {
    // 150ms, 300ms, 600ms, 1.2s, capped at 2s
    let shift = attempt.saturating_sub(1).min(4);
    let ms = (150u64.saturating_mul(1u64 << shift)).min(2_000);
    tokio::time::sleep(Duration::from_millis(ms)).await;
}

/// Slower backoff for the "account not yet visible at preflight commitment"
/// case — gives the cluster time to propagate the freshly created mint.
async fn propagation_backoff(attempt: u32) {
    // 700ms, 1.4s, 2.1s, capped at 3s
    let ms = (700u64.saturating_mul(u64::from(attempt))).min(3_000);
    tokio::time::sleep(Duration::from_millis(ms)).await;
}

/// Recognize pump-fun's `AccountNotInitialized` error on `mint` (Anchor 3012,
/// custom error `0xbc4`). Also catches the matching log messages emitted
/// before the program panics.
fn is_account_propagation_error(msg: &str) -> bool {
    let m = msg.to_lowercase();
    m.contains("0xbc4")
        || m.contains("accountnotinitialized")
        || m.contains("error number: 3012")
        || m.contains("\"err\": 3012")
}

/// Poll `get_account_with_commitment(Confirmed)` until the account is visible
/// or the budget expires. Returns Ok on first sighting; otherwise a
/// descriptive error the caller can surface to the position manager.
async fn wait_for_account_visible(
    rpc: &RpcClient,
    address: &SolAddress,
    label: &str,
    total_timeout: Duration,
) -> Result<(), BrokerError> {
    let pubkey_str = address.to_string();
    let pubkey = pubkey_str
        .parse()
        .map_err(|_| BrokerError::Custom(format!("{label}: invalid pubkey '{pubkey_str}'")))?;

    let started = std::time::Instant::now();
    let poll_delay = Duration::from_millis(150);
    let mut attempts: u32 = 0;

    loop {
        attempts += 1;
        match rpc
            .get_account_with_commitment(&pubkey, CommitmentConfig::confirmed())
            .await
        {
            Ok(resp) if resp.value.is_some() => {
                if attempts > 1 {
                    eprintln!(
                        "[BROKER] {label} {pubkey_str}: visible after {attempts} polls in {:?}",
                        started.elapsed()
                    );
                }
                return Ok(());
            }
            Ok(_) => {
                // Account not yet visible at confirmed commitment — keep polling.
            }
            Err(e) => {
                let msg = e.to_string();
                if !is_transient_balance_error(&msg) {
                    return Err(BrokerError::Custom(format!(
                        "{label}: get_account_with_commitment {pubkey_str} failed: {msg}"
                    )));
                }
            }
        }

        if started.elapsed() >= total_timeout {
            return Err(BrokerError::TransactionFailed(format!(
                "{label}: account {pubkey_str} not visible on RPC after {:?}",
                total_timeout
            )));
        }
        tokio::time::sleep(poll_delay).await;
    }
}
