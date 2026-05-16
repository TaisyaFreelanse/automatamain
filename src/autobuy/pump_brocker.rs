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
    launchpads::pump::general::bounding_curve,
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

        // Derive our ATA so we can read its raw token balance before/after the
        // BUY and report the *actual* fill in the receipt. Without this the
        // manager would size partial sells off the slippage floor
        // (`min_token_out_f`), which severely under-counts the real position
        // and leaves most tokens stranded in the ATA after a "100% sell".
        let (_ata_sol, ata_str) = derive_ata_address(&self.wallet_address, &mint);
        // ATA may not exist yet (idempotent create runs in this same tx), so
        // absence/error -> 0. Decimals are not needed pre-send.
        let pre_raw: u64 = fetch_token_account_raw(&self.rpc_client, &ata_str)
            .await
            .unwrap_or(0);

        // pump-fun derives `creator_vault` from `bonding_curve.creator`, NOT
        // from the user / CreateEvent.creator. That on-chain field can be
        // rewritten by `set_metaplex_creator` or seeded from backend data for
        // historical coins, so cached CreateEvent values mismatch and the BUY
        // fails with `ConstraintSeeds` (Anchor 2006 / 0xbc6+). We read the
        // authoritative value off the bonding curve account right before
        // submit and fall back to the cached creator only if the RPC read
        // fails for some reason.
        let creator_for_vault =
            match fetch_bonding_curve_creator(&self.rpc_client, &mint).await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!(
                        "[BROKER] fallback to cached creator for {mint}: bonding_curve read failed: {e}"
                    );
                    pool.creators()[0]
                }
            };

        let ix = PumpInstruction::buy_exact_in(
            mint.into(),
            self.wallet_address.into(),
            creator_for_vault.into(),
            TokenProgram2022::PROGRAM,
            sol_amount_in,
            min_token_out,
        );

        let mut ixs = self.compute_budget_prelude();
        ixs.push(create_ata_ix.into());
        ixs.push(ix.into());

        let sig_str = self.send_with_retries(ixs, "BUY").await?;

        // Optimistically debit so subsequent `balance_sol()` calls reflect the
        // spend without waiting on the periodic RPC refresh.
        {
            let mut bal = self.balance.lock().unwrap();
            *bal -= amount_sol;
        }

        // Resolve the *actual* fill from the chain by reading the ATA balance
        // delta. We poll until the post-balance exceeds the pre-balance (or
        // the timeout expires) and convert the raw lamport-equivalent diff
        // back to a UI float using on-chain decimals. This replaces the old
        // behaviour of reporting `min_token_out_f` (slippage floor), which
        // caused the position manager to size partial TP/SL sells off a
        // value that was orders of magnitude smaller than what we actually
        // bought, leaving most tokens stranded in the ATA on "100% exit".
        let actual_tokens_received = match poll_token_balance_increase(
            &self.rpc_client,
            &ata_str,
            pre_raw,
            Duration::from_millis(15_000),
        )
        .await
        {
            Ok((post_raw, decimals)) => {
                let delta_raw = post_raw.saturating_sub(pre_raw);
                let scale = 10u64.pow(decimals as u32) as f64;
                let actual = delta_raw as f64 / scale;
                eprintln!(
                    "[BROKER BUY] {mint}: filled tokens={:.6} (raw_delta={}, decimals={}, \
                     min_token_out={:.6})",
                    actual, delta_raw, decimals, min_token_out_f,
                );
                actual
            }
            Err(e) => {
                eprintln!(
                    "[BROKER BUY] {mint}: ATA balance delta unavailable ({e}); \
                     falling back to min_token_out={:.6} for receipt",
                    min_token_out_f,
                );
                min_token_out_f
            }
        };

        Ok(BuyReceipt {
            sol_spent: amount_sol,
            tokens_received: actual_tokens_received,
            signature: Some(sig_str),
        })
    }

    async fn sell(
        &self,
        mint: SolAddress,
        token_amount: f64,
        pool: &dyn Pool,
        close_account_after: bool,
    ) -> Result<SellReceipt, BrokerError> {
        // Derive the ATA up-front: needed for both the optional CloseAccount
        // ix and (on full exits) for sizing the SELL off the *current* raw
        // balance — which is the only number that lets `CloseAccount`
        // succeed atomically (Token-2022 refuses to close a non-empty
        // account).
        let (ata_sol, ata_str) = derive_ata_address(&self.wallet_address, &mint);

        // Resolve actual token amount dynamically:
        //   * full exit  -> read raw ATA balance now and sell exactly that;
        //   * partial    -> trust the manager's value;
        //   * fallback   -> WS-tracked balance if manager passed 0.
        let actual_token_amount = if close_account_after {
            match fetch_token_account_raw_with_decimals(&self.rpc_client, &ata_str).await {
                Ok((raw, decimals)) if raw > 0 => {
                    let scale = 10u64.pow(decimals as u32) as f64;
                    let chain_balance = raw as f64 / scale;
                    if (chain_balance - token_amount).abs() > token_amount.max(1.0) * 0.01 {
                        eprintln!(
                            "[BROKER SELL] {mint}: full-exit chain balance ({:.6}) differs from \
                             manager request ({:.6}) by >1%; using chain balance to allow \
                             atomic ATA close",
                            chain_balance, token_amount,
                        );
                    }
                    chain_balance
                }
                Ok(_) => {
                    eprintln!(
                        "[BROKER SELL] {mint}: full-exit requested but ATA already empty \
                         on-chain; will skip SELL and just close the account"
                    );
                    0.0
                }
                Err(e) => {
                    eprintln!(
                        "[BROKER SELL] {mint}: full-exit ATA balance read failed ({e}); \
                         falling back to manager-supplied amount {:.6}",
                        token_amount,
                    );
                    token_amount.max(0.0)
                }
            }
        } else {
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

        // For partial sells we still require a positive amount — selling
        // nothing partially is a logic error. For full exits, an already
        // empty ATA is fine: we'll just close it.
        if !close_account_after && actual_token_amount <= 0.0 {
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
            "[BROKER SELL] mint={} tokens={:.6} expected={:.6} SOL (price={:.9}) \
             min_out=0 close_ata={} (slippage waived for guaranteed exit)",
            mint, actual_token_amount, expected_sol, price_sol_per_token, close_account_after,
        );

        let mut ixs = self.compute_budget_prelude();

        // Skip the SELL ix only if this is a full exit AND there's literally
        // nothing left on-chain to sell. CloseAccount is still appended so we
        // recover rent.
        let needs_sell_ix = actual_token_amount > 0.0;
        if needs_sell_ix {
            let token_amount_in = sips::helper::Amount::<6>::from_float(actual_token_amount);
            let min_sol_out = sips::helper::Amount::<9>::from_float(min_sol_out_f);

            // Same on-chain `creator` lookup as in BUY — see the comment
            // there for the rationale. Sell uses the same `creator_vault`
            // derivation.
            let creator_for_vault =
                match fetch_bonding_curve_creator(&self.rpc_client, &mint).await {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!(
                            "[BROKER] fallback to cached creator for {mint}: bonding_curve read failed: {e}"
                        );
                        pool.creators()[0]
                    }
                };

            let ix = PumpInstruction::sell(
                mint.into(),
                self.wallet_address.into(),
                creator_for_vault.into(),
                TokenProgram2022::PROGRAM,
                token_amount_in,
                min_sol_out,
            );

            ixs.push(ix.into());
        }

        // Atomic rent recovery on full exits. Token-2022 `CloseAccount`
        // refunds the rent-exempt SOL deposit (~0.00203928 SOL per token
        // account) back to `destination` (our wallet) and burns the
        // account. It only succeeds if the account's token balance is 0,
        // which is why we just sized the SELL off the on-chain raw balance.
        // Without this, every new position permanently locks rent and the
        // wallet bleeds SOL even on a flat P&L.
        if close_account_after {
            let close_ix = TokenProgram2022::close_account(
                ata_sol.into(),
                self.wallet_address.into(),
                self.wallet_address.into(),
            );
            ixs.push(close_ix.into());
            eprintln!("[BROKER SELL] {mint}: appending CloseAccount(ata={ata_str}) for rent refund");
        }

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

/// Read the `creator` field directly from a pump-fun bonding_curve account.
///
/// Layout (Anchor): 8-byte discriminator + 5 × u64 (virtual/real reserves +
/// supply) + 1-byte `complete` flag + 32-byte `creator` Pubkey. So `creator`
/// lives at byte offset 49..81 of the account data. Newer pump-fun bonding
/// curves are extended to 150 bytes (see pump-public-docs), but the field
/// position is stable.
///
/// We deliberately ignore CreateEvent.creator and PumpPool.creators() because
/// `set_metaplex_creator` and the backend creator-seed path can mutate this
/// value after creation; only the on-chain account is authoritative for the
/// `creator_vault` PDA seeds the program checks.
async fn fetch_bonding_curve_creator(
    rpc: &RpcClient,
    mint: &SolAddress,
) -> Result<SolAddress, BrokerError> {
    let curve_addr = bounding_curve(mint).0;
    let curve_pk_str = curve_addr.to_string();
    let curve_pk = curve_pk_str
        .parse()
        .map_err(|_| BrokerError::Custom(format!("invalid bonding curve pubkey: {curve_pk_str}")))?;

    let resp = rpc
        .get_account_with_commitment(&curve_pk, CommitmentConfig::confirmed())
        .await
        .map_err(|e| BrokerError::Custom(format!("get_account bonding_curve {curve_pk_str}: {e}")))?;

    let account = resp
        .value
        .ok_or_else(|| BrokerError::Custom(format!("bonding curve {curve_pk_str} not found")))?;

    const CREATOR_OFFSET: usize = 8 + 8 * 5 + 1;
    const CREATOR_LEN: usize = 32;
    if account.data.len() < CREATOR_OFFSET + CREATOR_LEN {
        return Err(BrokerError::Custom(format!(
            "bonding_curve data too short: {} bytes, need at least {}",
            account.data.len(),
            CREATOR_OFFSET + CREATOR_LEN
        )));
    }
    let mut bytes = [0u8; CREATOR_LEN];
    bytes.copy_from_slice(&account.data[CREATOR_OFFSET..CREATOR_OFFSET + CREATOR_LEN]);
    Ok(SolAddress::new_from_array(bytes))
}

/// Derive the SPL associated token account address for `(wallet, mint)`
/// under the Token-2022 program (which pump-fun uses). Returns both the
/// typed `SolAddress` (for ix-building) and its base58 string form (for RPC
/// calls that go through string parsing — avoids hard-coupling this module
/// to a specific `solana-pubkey` version).
fn derive_ata_address(wallet: &SolAddress, mint: &SolAddress) -> (SolAddress, String) {
    let wallet_sips: sips::address::Address = (*wallet).into();
    let mint_sips: sips::address::Address = (*mint).into();
    let (ata_sips, _bump) = sips::helper::ata(
        &wallet_sips,
        &TokenProgram2022::PROGRAM,
        &mint_sips,
    );
    let ata_sol: SolAddress = ata_sips.into();
    let s = ata_sol.to_string();
    (ata_sol, s)
}

/// Read the raw token amount of a given ATA at `confirmed` commitment.
/// Errors if the account does not exist or is not parseable as a token
/// account (caller usually treats that as "balance == 0").
async fn fetch_token_account_raw(rpc: &RpcClient, ata: &str) -> Result<u64, BrokerError> {
    let pk = ata
        .parse()
        .map_err(|_| BrokerError::Custom(format!("invalid ATA pubkey: {ata}")))?;
    let resp = rpc
        .get_token_account_balance_with_commitment(&pk, CommitmentConfig::confirmed())
        .await
        .map_err(|e| BrokerError::Custom(format!("get_token_account_balance {ata}: {e}")))?;
    resp.value
        .amount
        .parse::<u64>()
        .map_err(|e| BrokerError::Custom(format!("parse token amount {ata}: {e}")))
}

/// Read the raw token amount + decimals of an ATA at `confirmed` commitment.
async fn fetch_token_account_raw_with_decimals(
    rpc: &RpcClient,
    ata: &str,
) -> Result<(u64, u8), BrokerError> {
    let pk = ata
        .parse()
        .map_err(|_| BrokerError::Custom(format!("invalid ATA pubkey: {ata}")))?;
    let resp = rpc
        .get_token_account_balance_with_commitment(&pk, CommitmentConfig::confirmed())
        .await
        .map_err(|e| BrokerError::Custom(format!("get_token_account_balance {ata}: {e}")))?;
    let raw = resp
        .value
        .amount
        .parse::<u64>()
        .map_err(|e| BrokerError::Custom(format!("parse token amount {ata}: {e}")))?;
    Ok((raw, resp.value.decimals))
}

/// Poll the ATA's token balance until it exceeds `pre_raw` (i.e. our BUY tx
/// has been confirmed and credited the ATA) or the total timeout elapses.
/// Returns `(post_raw, decimals)`.
///
/// Returning the on-chain `decimals` (rather than hardcoding pump's "6")
/// keeps the receipt accurate even if a token uses a non-standard precision.
async fn poll_token_balance_increase(
    rpc: &RpcClient,
    ata: &str,
    pre_raw: u64,
    total_timeout: Duration,
) -> Result<(u64, u8), BrokerError> {
    let started = std::time::Instant::now();
    let poll_delay = Duration::from_millis(400);
    let mut attempts: u32 = 0;
    let mut last_seen: Option<(u64, u8)> = None;
    let mut last_err: Option<String> = None;

    loop {
        attempts += 1;
        match fetch_token_account_raw_with_decimals(rpc, ata).await {
            Ok((raw, decimals)) => {
                last_seen = Some((raw, decimals));
                if raw > pre_raw {
                    return Ok((raw, decimals));
                }
            }
            Err(e) => {
                // ATA may simply not exist yet right after CreateIdempotent —
                // keep polling until the BUY ix actually credits it.
                last_err = Some(e.to_string());
            }
        }

        if started.elapsed() >= total_timeout {
            if let Some((raw, _decimals)) = last_seen
                && raw == pre_raw
            {
                return Err(BrokerError::Custom(format!(
                    "ATA {ata} balance did not increase after {attempts} polls in {:?} \
                     (pre={pre_raw}, post={raw})",
                    total_timeout
                )));
            }
            return Err(BrokerError::Custom(format!(
                "ATA {ata} balance read failed after {attempts} polls in {:?}: {}",
                total_timeout,
                last_err.unwrap_or_else(|| "no successful read".into()),
            )));
        }
        tokio::time::sleep(poll_delay).await;
    }
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
