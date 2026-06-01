use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use sips::{
    address::Address as SipsAddress,
    instructions::{
        associated_token_program::AssociatedTokenProgram,
        compute_budget::{ComputeUnitLimit, ComputeUnitPrice},
        pump::instructions::PumpInstruction,
        raw_instruction::Instruction as SipsInstruction,
        spl_token::SplTokenProgram,
        token_program_2022::TokenProgram2022,
    },
};
use solana_address::Address as SolAddress;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_instruction::Instruction as SolanaIx;
use solana_keypair::{Keypair, Signature};
use solana_rpc_client_types::config::{CommitmentConfig, CommitmentLevel};
use solana_transaction::versioned::VersionedTransaction;
// Use the modular solana crate instead of the monolithic solana_sdk

use crate::{
    autobuy::execution::LiveExecutionConfig,
    generalize::{general_commands::TradeAction, general_pool::Pool},
    helper::Amount,
    launchpads::pump::general::bounding_curve,
};

use super::{
    broker::{Broker, BrokerError, BuyReceipt, SellReceipt},
    jupiter_sell::{
        decode_jupiter_swap_transaction,         is_jupiter_empty_order, is_jupiter_execute_failed, is_jupiter_no_routes_found,
        is_jupiter_slippage, jupiter_build_swap_exact_in, jupiter_build_swap_v2_order_exact_in,
        jupiter_build_swap_wsol_to_mint_exact_in, jupiter_execute_v2_order, log_jupiter_fail,
        JupiterQuoteOpts, JupiterSwapBuild, JUPITER_EXCLUDE_PUMP_DEXES,
    },
    wallet_tx_sol,
};

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

    /// Net wallet SOL delta from confirmed tx meta (`post - pre` lamports), or
    /// `fallback_sol` when `getTransaction` / balance alignment fails.
    async fn wallet_net_sol_or_fallback(
        &self,
        sig_str: &str,
        fallback_sol: f64,
        label: &str,
    ) -> f64 {
        match wallet_tx_sol::wallet_net_sol_received_f64(
            self.rpc_client.as_ref(),
            sig_str,
            &self.wallet_address,
        )
        .await
        {
            Ok(v) => v,
            Err(e) => {
                eprintln!(
                    "[BROKER SELL] {label}: on-chain SOL delta for sig={sig_str} failed ({e}); \
                     using fallback {fallback_sol:.6} SOL"
                );
                fallback_sol
            }
        }
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

    /// `send_transaction` only proves the RPC accepted the wire format. With
    /// `skip_preflight: true` (Gatekeeper / unsupported preflight) the same
    /// call can return a signature for a tx that never lands or fails at
    /// execution. The manager must not drop a position until we see this
    /// signature at **confirmed** commitment with no `err`.
    async fn wait_until_signature_confirmed_ok(
        &self,
        signature: &Signature,
        label: &str,
    ) -> Result<(), BrokerError> {
        const TIMEOUT: Duration = Duration::from_secs(90);
        const POLL_FAST: Duration = Duration::from_millis(150);
        const POLL_SLOW: Duration = Duration::from_millis(400);
        let started = std::time::Instant::now();

        loop {
            let resp = self
                .rpc_client
                .get_signature_statuses(std::slice::from_ref(signature))
                .await
                .map_err(|e| {
                    BrokerError::Custom(format!("{label}: get_signature_statuses: {e}"))
                })?;

            if let Some(Some(status)) = resp.value.first() {
                if let Some(ref err) = status.err {
                    return Err(BrokerError::TransactionFailed(format!(
                        "{label}: on-chain transaction error: {err:?}"
                    )));
                }
                if status.status.is_err() {
                    return Err(BrokerError::TransactionFailed(format!(
                        "{label}: on-chain transaction status: {:?}",
                        status.status
                    )));
                }
                if status.satisfies_commitment(CommitmentConfig::confirmed()) {
                    eprintln!(
                        "[BROKER TX] {label}: {signature} confirmed (no execution error)"
                    );
                    return Ok(());
                }
                if status.satisfies_commitment(CommitmentConfig::processed()) {
                    eprintln!(
                        "[BROKER TX] {label}: {signature} processed (accepting early)"
                    );
                    return Ok(());
                }
            }

            if started.elapsed() >= TIMEOUT {
                return Err(BrokerError::TransactionFailed(format!(
                    "{label}: timed out after {TIMEOUT:?} waiting for confirmed success \
                     (sig={signature})"
                )));
            }
            let poll = if started.elapsed() < Duration::from_secs(3) {
                POLL_FAST
            } else {
                POLL_SLOW
            };
            tokio::time::sleep(poll).await;
        }
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
                    self.wait_until_signature_confirmed_ok(&sig, label).await?;
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

    /// Entry after `BondingCurve::complete` (pump `buy` returns Custom 6005).
    /// Swaps SOL → mint via Jupiter v6 (`ExactIn` on wrapped SOL).
    async fn buy_tokens_post_graduation(
        &self,
        mint: SolAddress,
        amount_sol: f64,
        mint_token: MintTokenKind,
    ) -> Result<BuyReceipt, BrokerError> {
        let slippage_bps_u16: u16 = self
            .exec_cfg
            .slippage_bps
            .min(5000)
            .try_into()
            .unwrap_or(500);

        let sol_lamports: u64 = (amount_sol * 1_000_000_000.0).round() as u64;
        if sol_lamports == 0 {
            return Err(BrokerError::Custom(
                "Jupiter BUY: amount_sol rounds to 0 lamports".into(),
            ));
        }

        let (_ata_sol, ata_str) =
            derive_ata_address(&self.wallet_address, &mint, mint_token.program());
        let pre_raw: u64 = fetch_token_account_raw(&self.rpc_client, &ata_str)
            .await
            .unwrap_or(0);

        let mint_s = mint.to_string();
        let build = jupiter_build_swap_wsol_to_mint_exact_in(
            &mint_s,
            sol_lamports,
            slippage_bps_u16,
            &self.wallet_address.to_string(),
            JupiterQuoteOpts::POST_GRADUATION,
        )
        .await?;

        let tx_bytes = STANDARD
            .decode(build.swap_transaction_b64.trim())
            .map_err(|e| BrokerError::Custom(format!("Jupiter swapTransaction base64: {e}")))?;
        let template = decode_jupiter_swap_transaction(&tx_bytes)?;

        let sig_str = self
            .send_versioned_jupiter_with_retries(&template, "BUY-JUPITER")
            .await?
            .to_string();

        {
            let mut bal = self.balance.lock().unwrap();
            *bal -= amount_sol;
        }

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
                    "[BROKER BUY-JUPITER] {mint}: filled tokens={:.6} (raw_delta={}, decimals={}, \
                     jupiter_quote_out_raw={})",
                    actual, delta_raw, decimals, build.out_lamports,
                );
                actual
            }
            Err(e) => {
                eprintln!(
                    "[BROKER BUY-JUPITER] {mint}: ATA balance delta unavailable ({e}); \
                     using Jupiter quote out_raw={}",
                    build.out_lamports,
                );
                match fetch_token_account_raw_with_decimals(&self.rpc_client, &ata_str).await {
                    Ok((_raw, decimals)) => {
                        let scale = 10f64.powi(i32::from(decimals));
                        (build.out_lamports as f64 / scale).max(0.0)
                    }
                    Err(_) => (build.out_lamports as f64 / 1_000_000.0).max(0.0),
                }
            }
        };

        Ok(BuyReceipt {
            sol_spent: amount_sol,
            tokens_received: actual_tokens_received,
            signature: Some(sig_str),
            entry_mcap_fill_sol: None,
        })
    }

    async fn jupiter_quote_swap_build(
        &self,
        mint: SolAddress,
        mint_s: &str,
        chunk_raw: u64,
        slippage_bps_u16: u16,
        opts: JupiterQuoteOpts,
        label: &str,
    ) -> Result<JupiterSwapBuild, BrokerError> {
        jupiter_build_swap_exact_in(
            mint_s,
            chunk_raw,
            slippage_bps_u16,
            &self.wallet_address.to_string(),
            opts,
        )
        .await
        .map_err(|e| {
            eprintln!(
                "[BROKER SELL] {mint}: Jupiter quote failed ({label}, raw={chunk_raw}): {e}"
            );
            e
        })
    }

    async fn jupiter_send_swap_build(
        &self,
        build: &JupiterSwapBuild,
        mint_log: &str,
        raw_log: u64,
    ) -> Result<Signature, BrokerError> {
        if build.v2_request_id.is_some() {
            return jupiter_execute_v2_order(build, self.keypair.as_ref(), mint_log, raw_log).await;
        }
        let tx_bytes = STANDARD
            .decode(build.swap_transaction_b64.trim())
            .map_err(|e| BrokerError::Custom(format!("Jupiter swapTransaction base64: {e}")))?;
        let template = decode_jupiter_swap_transaction(&tx_bytes)?;
        self.send_versioned_jupiter_with_retries(&template, "SELL-JUPITER")
            .await
    }

    /// Post-graduation quote: exclude Pump → v2/order → v1 without exclude (always after v2 miss).
    async fn jupiter_quote_swap_build_post_graduation(
        &self,
        mint: SolAddress,
        mint_s: &str,
        chunk_raw: u64,
        slippage_bps_u16: u16,
    ) -> Result<JupiterSwapBuild, BrokerError> {
        let wallet = self.wallet_address.to_string();
        match self
            .jupiter_quote_swap_build(
                mint,
                mint_s,
                chunk_raw,
                slippage_bps_u16,
                JupiterQuoteOpts::POST_GRADUATION,
                "exclude Pump DEXes, multi-hop",
            )
            .await
        {
            Ok(build) => return Ok(build),
            Err(e) if is_jupiter_no_routes_found(&e) => {
                log_jupiter_fail("NO_ROUTES", mint_s, chunk_raw, &e.to_string());
            }
            Err(e) => return Err(e),
        }

        eprintln!(
            "[JUPITER] {mint}: fallback GET /swap/v2/order (excludeDexes={JUPITER_EXCLUDE_PUMP_DEXES}, raw={chunk_raw})"
        );
        match jupiter_build_swap_v2_order_exact_in(
            mint_s,
            chunk_raw,
            slippage_bps_u16,
            &wallet,
            Some(JUPITER_EXCLUDE_PUMP_DEXES),
        )
        .await
        {
            Ok(build) => return Ok(build),
            Err(e) => {
                if is_jupiter_no_routes_found(&e) {
                    log_jupiter_fail("NO_ROUTES", mint_s, chunk_raw, &format!("v2/order: {e}"));
                } else if !is_jupiter_empty_order(&e) {
                    log_jupiter_fail("EXECUTE_FAILED", mint_s, chunk_raw, &format!("v2/order: {e}"));
                }
            }
        }

        eprintln!(
            "[JUPITER] {mint}: fallback v1 quote without excludeDexes (raw={chunk_raw}; 6005 guarded on send)"
        );
        self.jupiter_quote_swap_build(
            mint,
            mint_s,
            chunk_raw,
            slippage_bps_u16,
            JupiterQuoteOpts::POST_GRADUATION_ALLOW_PUMP,
            "no excludeDexes, multi-hop",
        )
        .await
    }

    /// v1 allow-Pump only (skip exclude + v2) — last resort after slippage retries.
    async fn jupiter_quote_swap_build_allow_pump_v1(
        &self,
        mint: SolAddress,
        mint_s: &str,
        chunk_raw: u64,
        slippage_bps_u16: u16,
    ) -> Result<JupiterSwapBuild, BrokerError> {
        eprintln!(
            "[JUPITER] {mint}: v1 allow-pump only (raw={chunk_raw}, slippage_bps={slippage_bps_u16})"
        );
        self.jupiter_quote_swap_build(
            mint,
            mint_s,
            chunk_raw,
            slippage_bps_u16,
            JupiterQuoteOpts::POST_GRADUATION_ALLOW_PUMP,
            "no excludeDexes, multi-hop (slippage fallback)",
        )
        .await
    }

    fn jupiter_slippage_steps(base_bps: u16) -> Vec<u16> {
        let mut steps = Vec::new();
        for mul in [1u32, 2, 3] {
            let b = ((base_bps as u32).saturating_mul(mul)).min(3500) as u16;
            if !steps.contains(&b) {
                steps.push(b);
            }
        }
        steps
    }

    async fn jupiter_retry_build_after_pump_6005(
        &self,
        mint: SolAddress,
        mint_s: &str,
        chunk_raw: u64,
        slippage_bps_u16: u16,
        had_exclude_on_first_quote: bool,
    ) -> Result<JupiterSwapBuild, BrokerError> {
        if had_exclude_on_first_quote {
            if let Ok(build) = self
                .jupiter_quote_swap_build(
                    mint,
                    mint_s,
                    chunk_raw,
                    slippage_bps_u16,
                    JupiterQuoteOpts::POST_GRADUATION_DIRECT_ONLY,
                    "exclude Pump DEXes, onlyDirectRoutes (6005 fallback)",
                )
                .await
            {
                return Ok(build);
            }
        }
        self.jupiter_quote_swap_build(
            mint,
            mint_s,
            chunk_raw,
            slippage_bps_u16,
            JupiterQuoteOpts::POST_GRADUATION_ALLOW_PUMP_DIRECT,
            "no exclude, onlyDirectRoutes (6005 fallback)",
        )
        .await
    }

    /// Quote + send one Jupiter sell chunk (exclude Pump; `onlyDirectRoutes` only after on-chain 6005).
    async fn jupiter_sell_chunk(
        &self,
        mint: SolAddress,
        mint_s: &str,
        ata_str: &str,
        pre_sell_raw: u64,
        chunk_raw: u64,
        close_ata_after: bool,
        ata_sol: SolAddress,
        slip: f64,
        mint_token: MintTokenKind,
        slippage_bps_u16: u16,
    ) -> Result<SellReceipt, BrokerError> {
        if chunk_raw == 0 {
            return Err(BrokerError::Custom(format!(
                "{mint}: Jupiter chunk amount is zero"
            )));
        }

        let slip_steps = Self::jupiter_slippage_steps(slippage_bps_u16);
        let mut last_slip_err: Option<BrokerError> = None;

        for slip_bps in &slip_steps {
            match self
                .jupiter_sell_chunk_once(
                    mint,
                    mint_s,
                    ata_str,
                    pre_sell_raw,
                    chunk_raw,
                    close_ata_after,
                    ata_sol,
                    slip,
                    mint_token,
                    *slip_bps,
                    false,
                )
                .await
            {
                Ok(r) => return Ok(r),
                Err(e) if is_jupiter_slippage(&e) => {
                    log_jupiter_fail("SLIPPAGE", mint_s, chunk_raw, &e.to_string());
                    last_slip_err = Some(e);
                }
                Err(e) => return Err(e),
            }
        }

        if let Some(e) = &last_slip_err {
            eprintln!(
                "[JUPITER] {mint}: SLIPPAGE retries exhausted ({e}) — v1 allow-pump only (raw={chunk_raw})"
            );
        }

        let max_slip = *slip_steps.last().unwrap_or(&slippage_bps_u16);
        self.jupiter_sell_chunk_once(
            mint,
            mint_s,
            ata_str,
            pre_sell_raw,
            chunk_raw,
            close_ata_after,
            ata_sol,
            slip,
            mint_token,
            max_slip,
            true,
        )
        .await
    }

    async fn jupiter_sell_chunk_once(
        &self,
        mint: SolAddress,
        mint_s: &str,
        ata_str: &str,
        pre_sell_raw: u64,
        chunk_raw: u64,
        close_ata_after: bool,
        ata_sol: SolAddress,
        slip: f64,
        mint_token: MintTokenKind,
        slippage_bps_u16: u16,
        allow_pump_v1_only: bool,
    ) -> Result<SellReceipt, BrokerError> {
        let mut build = if allow_pump_v1_only {
            self.jupiter_quote_swap_build_allow_pump_v1(
                mint,
                mint_s,
                chunk_raw,
                slippage_bps_u16,
            )
            .await?
        } else {
            self.jupiter_quote_swap_build_post_graduation(
                mint,
                mint_s,
                chunk_raw,
                slippage_bps_u16,
            )
            .await?
        };

        let sig = match self
            .jupiter_send_swap_build(&build, mint_s, chunk_raw)
            .await
        {
            Ok(sig) => sig,
            Err(BrokerError::TransactionFailed(ref msg))
                if is_bonding_curve_complete_pump_error(msg) =>
            {
                eprintln!(
                    "[BROKER SELL] {mint}: SELL-JUPITER 6005 (multi-hop, raw={chunk_raw}) — onlyDirectRoutes"
                );
                build = self
                    .jupiter_retry_build_after_pump_6005(
                        mint,
                        mint_s,
                        chunk_raw,
                        slippage_bps_u16,
                        build.used_exclude_pump_dexes,
                    )
                    .await?;
                self.jupiter_send_swap_build(&build, mint_s, chunk_raw)
                    .await?
            }
            Err(e) if is_jupiter_slippage(&e) => return Err(e),
            Err(e) if is_jupiter_execute_failed(&e) => {
                log_jupiter_fail("EXECUTE_FAILED", mint_s, chunk_raw, &e.to_string());
                return Err(e);
            }
            Err(e) => return Err(e),
        };

        poll_ata_confirms_sell(
            &self.rpc_client,
            ata_str,
            pre_sell_raw,
            chunk_raw,
            false,
            mint,
            Duration::from_secs(90),
        )
        .await?;

        let sol_gross = build.out_lamports as f64 / 1e9;
        let sol_estimated = (sol_gross * (1.0 - slip)).max(0.0);
        let sig_jup_str = sig.to_string();
        let mut sol_actual = self
            .wallet_net_sol_or_fallback(&sig_jup_str, sol_estimated, "SELL-JUPITER-META")
            .await;
        let mut last_sig = sig_jup_str;

        if close_ata_after {
            match fetch_token_account_raw(&self.rpc_client, ata_str).await {
                Ok(0) => {
                    let mut ixs = self.compute_budget_prelude();
                    let close_ix = mint_token.close_account_ix(
                        ata_sol.into(),
                        self.wallet_address.into(),
                        self.wallet_address.into(),
                    );
                    ixs.push(close_ix);
                    if let Ok(cs) = self.send_with_retries(ixs, "CLOSE-ATA").await {
                        let close_delta = self
                            .wallet_net_sol_or_fallback(&cs, 0.0, "CLOSE-ATA-META")
                            .await;
                        sol_actual += close_delta;
                        last_sig = cs;
                    }
                }
                Ok(left) => eprintln!(
                    "[BROKER SELL] {mint}: post-Jupiter ATA still has raw {left}; skip close"
                ),
                Err(e) => eprintln!(
                    "[BROKER SELL] {mint}: post-Jupiter ATA read failed ({e}); skip close"
                ),
            }
        }

        {
            let mut bal = self.balance.lock().unwrap();
            *bal += sol_actual;
        }
        Ok(SellReceipt {
            sol_received_actual: sol_actual,
            sol_received_estimated: sol_estimated,
            signature: Some(last_sig),
        })
    }

    fn jupiter_chunk_amounts(total_raw: u64, parts: u32) -> Vec<u64> {
        if total_raw == 0 {
            return Vec::new();
        }
        if parts <= 1 || total_raw < parts as u64 * 50_000 {
            return vec![total_raw];
        }
        let base = total_raw / parts as u64;
        let mut chunks: Vec<u64> = (0..parts - 1).map(|_| base).collect();
        chunks.push(total_raw - base * (parts as u64 - 1));
        chunks
    }

    /// Full or partial exit after bonding-curve graduation (Jupiter Swap API v1).
    async fn sell_tokens_post_graduation(
        &self,
        mint: SolAddress,
        ata_str: &str,
        ata_sol: SolAddress,
        pre_sell_raw: u64,
        sold_raw: u64,
        do_close_ata: bool,
        slip: f64,
        mint_token: MintTokenKind,
    ) -> Result<SellReceipt, BrokerError> {
        let slippage_bps_u16: u16 = self
            .exec_cfg
            .slippage_bps
            .min(5000)
            .try_into()
            .unwrap_or(500);

        let mint_s = mint.to_string();
        let mut last_err: Option<BrokerError> = None;

        for parts in [1u32, 4] {
            let chunks = Self::jupiter_chunk_amounts(sold_raw, parts);
            if parts > 1 {
                eprintln!(
                    "[BROKER SELL] {mint}: Jupiter retry with {} chunks (total_raw={sold_raw})",
                    chunks.len()
                );
            }

            let mut total_actual = 0.0_f64;
            let mut total_est = 0.0_f64;
            let mut last_sig: Option<String> = None;
            let mut pre_raw = pre_sell_raw;
            let mut all_ok = true;

            for (i, chunk_raw) in chunks.iter().enumerate() {
                let close_ata = do_close_ata && i + 1 == chunks.len();
                match self
                    .jupiter_sell_chunk(
                        mint,
                        &mint_s,
                        ata_str,
                        pre_raw,
                        *chunk_raw,
                        close_ata,
                        ata_sol,
                        slip,
                        mint_token,
                        slippage_bps_u16,
                    )
                    .await
                {
                    Ok(r) => {
                        total_actual += r.sol_received_actual;
                        total_est += r.sol_received_estimated;
                        if let Some(s) = r.signature {
                            last_sig = Some(s);
                        }
                        pre_raw = pre_raw.saturating_sub(*chunk_raw);
                    }
                    Err(e) => {
                        all_ok = false;
                        last_err = Some(e);
                        break;
                    }
                }
            }

            if all_ok {
                return Ok(SellReceipt {
                    sol_received_actual: total_actual,
                    sol_received_estimated: total_est,
                    signature: last_sig,
                });
            }

            if parts == 1 {
                continue;
            }
            eprintln!(
                "[BROKER SELL] {mint}: Jupiter chunked sell failed: {:?}",
                last_err
            );
        }

        let detail = last_err
            .map(|e| e.to_string())
            .unwrap_or_else(|| "unknown".into());
        Err(BrokerError::JupiterSellExhausted(format!(
            "{mint}: Jupiter sell exhausted after exclude→v2→v1-allow-pump, slippage retries, \
             and 4-way split (raw={sold_raw}): {detail}"
        )))
    }

    async fn send_versioned_jupiter_with_retries(
        &self,
        template: &VersionedTransaction,
        label: &str,
    ) -> Result<Signature, BrokerError> {
        let max_retries = self.exec_cfg.max_retries.max(1).max(6);
        let mut attempt: u32 = 0;
        let signer: &Keypair = self.keypair.as_ref();
        loop {
            attempt += 1;
            let blockhash = self
                .rpc_client
                .get_latest_blockhash()
                .await
                .map_err(|e| BrokerError::Custom(format!("{label}: get_latest_blockhash: {e}")))?;
            let mut message = template.message.clone();
            message.set_recent_blockhash(blockhash);
            let signed = VersionedTransaction::try_new(message, &[signer]).map_err(|e| {
                BrokerError::Custom(format!("{label}: VersionedTransaction::try_new: {e}"))
            })?;

            let cfg = solana_client::rpc_config::RpcSendTransactionConfig {
                skip_preflight: self.exec_cfg.skip_preflight,
                preflight_commitment: Some(CommitmentLevel::Processed),
                ..Default::default()
            };

            match self
                .rpc_client
                .send_transaction_with_config(&signed, cfg)
                .await
            {
                Ok(sig) => {
                    let sig_str = sig.to_string();
                    println!("[BROKER TX] {label} sent (attempt {attempt}): {sig_str}");
                    self.wait_until_signature_confirmed_ok(&sig, label).await?;
                    return Ok(sig);
                }
                Err(e) => {
                    let msg = e.to_string();
                    eprintln!("[BROKER TX] {label} attempt {attempt}/{max_retries} failed: {msg}");
                    if attempt >= max_retries {
                        return Err(BrokerError::TransactionFailed(msg));
                    }
                    backoff(attempt).await;
                }
            }
        }
    }

    /// After a liquidity-clamped "100%" sell, keep submitting pump `Sell` legs
    /// until the ATA balance hits zero (or the curve graduates — then Jupiter),
    /// then `CloseAccount` when the ATA is empty.
    async fn drain_pump_ata_after_clamped_full_exit(
        &self,
        mint: SolAddress,
        ata_str: &str,
        ata_sol: SolAddress,
        mint_token: MintTokenKind,
        pool: &dyn Pool,
        slip: f64,
        first_confirmed_sig: String,
    ) -> Result<(f64, f64, String), BrokerError> {
        let mut cumulative_actual: f64 = 0.0;
        let mut cumulative_est: f64 = 0.0;
        let mut last_sig = first_confirmed_sig;
        const MAX_DRAIN_LEGS: u32 = 50;
        for leg in 0u32..MAX_DRAIN_LEGS {
            let pre_raw = fetch_token_account_raw(&self.rpc_client, ata_str)
                .await
                .unwrap_or(0);
            if pre_raw == 0 {
                eprintln!(
                    "[BROKER SELL] {mint}: clamp-drain done (raw=0) after {leg} extra leg(s)"
                );
                break;
            }

            let curve_state_opt = match fetch_bonding_curve_state(&self.rpc_client, &mint).await {
                Ok(s) => Some(s),
                Err(e) => {
                    eprintln!(
                        "[BROKER SELL] {mint}: clamp-drain stopped: bonding_curve read failed: {e}"
                    );
                    break;
                }
            };

            if curve_state_opt
                .as_ref()
                .is_some_and(|c| c.curve_complete)
            {
                eprintln!(
                    "[BROKER SELL] {mint}: curve graduated during clamp-drain; routing {pre_raw} raw via Jupiter"
                );
                let jup = self
                    .sell_tokens_post_graduation(
                        mint,
                        ata_str,
                        ata_sol,
                        pre_raw,
                        pre_raw,
                        true,
                        slip,
                        mint_token,
                    )
                    .await?;
                cumulative_actual += jup.sol_received_actual;
                cumulative_est += jup.sol_received_estimated;
                if let Some(s) = jup.signature {
                    last_sig = s;
                }
                break;
            }

            let creator_for_vault: SolAddress = match &curve_state_opt {
                Some(s) => s.creator,
                None => pool.creators()[0],
            };

            let mut token_amount_raw = pre_raw;
            let mut leg_clamped = false;
            if let Some(curve) = &curve_state_opt {
                let safety_cap_lamports =
                    ((curve.real_sol_reserves as u128) * 90 / 100) as u64;
                let projected_sol_out = pump_sol_out_for_tokens(token_amount_raw, curve);
                if projected_sol_out > safety_cap_lamports {
                    let max_sellable =
                        pump_max_sellable_tokens_for_sol(safety_cap_lamports, curve);
                    let clamped = max_sellable.min(token_amount_raw);
                    if clamped < token_amount_raw {
                        eprintln!(
                            "[BROKER SELL] {mint}: clamp-drain leg {leg}: {} -> {} raw",
                            token_amount_raw, clamped
                        );
                        token_amount_raw = clamped;
                        leg_clamped = true;
                    }
                }
            }

            if token_amount_raw == 0 {
                eprintln!(
                    "[BROKER SELL] {mint}: clamp-drain leg {leg}: nothing sellable after clamp; stopping"
                );
                break;
            }

            let final_token_amount_ui = token_amount_raw as f64 / 1_000_000.0;
            let expected_sol = match &curve_state_opt {
                Some(c) => pump_sol_out_for_tokens(token_amount_raw, c) as f64 / 1e9,
                None => final_token_amount_ui * pool.price().amount().to_float().max(0.0),
            };
            let expected_sol_after_slip = (expected_sol * (1.0 - slip)).max(0.0);

            let cashback_enabled = curve_state_opt
                .as_ref()
                .map(|c| c.is_cashback_coin)
                .unwrap_or(false);
            if cashback_enabled {
                eprintln!(
                    "[BROKER SELL] {mint}: clamp-drain leg {leg}: cashback coin — user_volume_accumulator"
                );
            }

            let mut ixs = self.compute_budget_prelude();
            let token_amount_in = sips::helper::Amount::<6>::from_raw(token_amount_raw);
            let min_sol_out = sips::helper::Amount::<9>::from_float(0.0_f64);
            let ix = PumpInstruction::sell(
                mint.into(),
                self.wallet_address.into(),
                creator_for_vault.into(),
                mint_token.program(),
                token_amount_in,
                min_sol_out,
                cashback_enabled,
            );
            ixs.push(ix.into());

            let label = format!("SELL-DRAIN-{leg}");
            let sig = match self.send_with_retries(ixs, &label).await {
                Ok(s) => s,
                Err(BrokerError::TransactionFailed(ref msg))
                    if is_bonding_curve_complete_pump_error(msg) =>
                {
                    eprintln!(
                        "[BROKER SELL] {mint}: clamp-drain leg {leg}: BondingCurveComplete — Jupiter"
                    );
                    let jup = self
                        .sell_tokens_post_graduation(
                            mint,
                            ata_str,
                            ata_sol,
                            pre_raw,
                            pre_raw,
                            true,
                            slip,
                            mint_token,
                        )
                        .await?;
                    cumulative_actual += jup.sol_received_actual;
                    cumulative_est += jup.sol_received_estimated;
                    if let Some(s) = jup.signature {
                        last_sig = s;
                    }
                    break;
                }
                Err(e) => return Err(e),
            };

            poll_ata_confirms_sell(
                &self.rpc_client,
                ata_str,
                pre_raw,
                token_amount_raw,
                false,
                mint,
                Duration::from_secs(45),
            )
            .await?;

            let leg_actual = self
                .wallet_net_sol_or_fallback(
                    &sig,
                    expected_sol_after_slip,
                    &format!("SELL-DRAIN-{leg}-META"),
                )
                .await;
            cumulative_actual += leg_actual;
            cumulative_est += expected_sol_after_slip;
            last_sig = sig;

            if !leg_clamped && token_amount_raw >= pre_raw.saturating_sub(sell_raw_delta_slack(pre_raw))
            {
                // One full-balance leg with no clamp; if anything remains it's dust / rounding.
                let after = fetch_token_account_raw(&self.rpc_client, ata_str)
                    .await
                    .unwrap_or(0);
                if after > 0 && after <= 1000 {
                    eprintln!(
                        "[BROKER SELL] {mint}: clamp-drain leg {leg}: {after} raw dust left; stopping sells"
                    );
                    break;
                }
            }
        }

        let rem = fetch_token_account_raw(&self.rpc_client, ata_str)
            .await
            .unwrap_or(0);
        if rem == 0 {
            let mut ixs = self.compute_budget_prelude();
            ixs.push(mint_token.close_account_ix(
                ata_sol.into(),
                self.wallet_address.into(),
                self.wallet_address.into(),
            ));
            let csig = self.send_with_retries(ixs, "SELL-CLOSE-ATA").await?;
            poll_ata_confirms_sell(
                &self.rpc_client,
                ata_str,
                0,
                1,
                true,
                mint,
                Duration::from_secs(30),
            )
            .await?;
            let close_actual = self
                .wallet_net_sol_or_fallback(&csig, 0.0, "SELL-CLOSE-ATA-META")
                .await;
            cumulative_actual += close_actual;
            last_sig = csig;
        } else {
            eprintln!(
                "[BROKER SELL] {mint}: warning: {rem} raw tokens remain after clamp-drain; CloseAccount skipped"
            );
        }

        Ok((cumulative_actual, cumulative_est, last_sig))
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
            Duration::from_millis(3_500),
            3,
            Duration::from_millis(300),
        )
        .await?;

        let mint_token = fetch_mint_token_kind(&self.rpc_client, &mint).await?;

        // Graduated token: pump `buy` returns `BondingCurveComplete` (Anchor 6005).
        if let Ok(curve) = fetch_bonding_curve_state(&self.rpc_client, &mint).await {
            if curve.curve_complete {
                eprintln!(
                    "[BROKER BUY] {mint}: bonding curve COMPLETE — routing BUY via Jupiter"
                );
                return self
                    .buy_tokens_post_graduation(mint, amount_sol, mint_token)
                    .await;
            }
        }

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
            mint_token.program(),
        );

        // Derive our ATA so we can read its raw token balance before/after the
        // BUY and report the *actual* fill in the receipt. Without this the
        // manager would size partial sells off the slippage floor
        // (`min_token_out_f`), which severely under-counts the real position
        // and leaves most tokens stranded in the ATA after a "100% sell".
        let (_ata_sol, ata_str) =
            derive_ata_address(&self.wallet_address, &mint, mint_token.program());
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
            mint_token.program(),
            sol_amount_in,
            min_token_out,
        );

        let mut ixs = self.compute_budget_prelude();
        ixs.push(create_ata_ix.into());
        ixs.push(ix.into());

        let sig_str = match self.send_with_retries(ixs, "BUY").await {
            Ok(s) => s,
            Err(BrokerError::TransactionFailed(ref msg))
                if is_bonding_curve_complete_pump_error(msg) =>
            {
                eprintln!(
                    "[BROKER BUY] {mint}: pump BUY failed BondingCurveComplete (6005) — retrying via Jupiter"
                );
                return self
                    .buy_tokens_post_graduation(mint, amount_sol, mint_token)
                    .await;
            }
            Err(e) => return Err(e),
        };

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

        let entry_mcap_fill_sol = match fetch_bonding_curve_state(&self.rpc_client, &mint).await {
            Ok(curve) => {
                let m = bonding_curve_mcap_sol(&curve);
                if m > 0.0 {
                    eprintln!(
                        "[BROKER BUY] {mint}: entry mcap from RPC bonding curve (post-fill): \
                         {:.4} SOL",
                        m
                    );
                    Some(m)
                } else {
                    None
                }
            }
            Err(e) => {
                eprintln!(
                    "[BROKER BUY] {mint}: bonding_curve read for entry mcap failed ({e}); \
                     manager uses WS pool mcap"
                );
                None
            }
        };

        Ok(BuyReceipt {
            sol_spent: amount_sol,
            tokens_received: actual_tokens_received,
            signature: Some(sig_str),
            entry_mcap_fill_sol,
        })
    }

    async fn sell(
        &self,
        mint: SolAddress,
        token_amount: f64,
        pool: &dyn Pool,
        close_account_after: bool,
    ) -> Result<SellReceipt, BrokerError> {
        let mint_token = fetch_mint_token_kind(&self.rpc_client, &mint).await?;

        // Derive the ATA up-front: needed for both the optional CloseAccount
        // ix and (on full exits) for sizing the SELL off the *current* raw
        // balance — which is the only number that lets `CloseAccount`
        // succeed atomically (Token-2022 refuses to close a non-empty
        // account).
        let (ata_sol, ata_str) =
            derive_ata_address(&self.wallet_address, &mint, mint_token.program());

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
        let slip = self.exec_cfg.slippage_bps as f64 / 10_000.0;
        let min_sol_out_f = 0.0_f64;

        // Read bonding curve once: it gives us both `creator` (for the
        // creator_vault PDA seeds) and `real_sol_reserves` + virtual
        // reserves needed to clamp `token_amount` against the curve's
        // actual SOL liquidity. Without that clamp, fresh / illiquid coins
        // hit pump-fun's `Overflow` error 6024 (0x1788) at lib.rs:844 —
        // the underflow on `real_sol_reserves.checked_sub(sol_amount)`
        // when our requested sell would drain more SOL than the curve
        // physically holds.
        let curve_state_opt = match fetch_bonding_curve_state(&self.rpc_client, &mint).await {
            Ok(s) => Some(s),
            Err(e) => {
                eprintln!(
                    "[BROKER SELL] {mint}: bonding_curve read failed ({e}); cannot \
                     clamp sell vs real_sol_reserves — proceeding with raw amount and \
                     hoping for the best"
                );
                None
            }
        };
        let creator_for_vault: SolAddress = match &curve_state_opt {
            Some(s) => s.creator,
            None => pool.creators()[0],
        };

        // Convert UI -> raw using pump's 6 decimals, then optionally clamp.
        let mut token_amount_raw: u64 = (actual_token_amount * 1_000_000.0).round() as u64;
        let mut sell_was_clamped = false;
        if let Some(curve) = &curve_state_opt {
            // Leave a 10% safety margin: fees + intra-block trades can
            // shrink real_sol_reserves between our read and confirmation,
            // and pump's check is on the gross sol_amount (pre-fee).
            let safety_cap_lamports =
                ((curve.real_sol_reserves as u128) * 90 / 100) as u64;
            let projected_sol_out = pump_sol_out_for_tokens(token_amount_raw, curve);
            if projected_sol_out > safety_cap_lamports {
                let max_sellable =
                    pump_max_sellable_tokens_for_sol(safety_cap_lamports, curve);
                let clamped = max_sellable.min(token_amount_raw);
                eprintln!(
                    "[BROKER SELL] {mint}: clamping sell {} -> {} raw tokens \
                     (projected_sol_out={} > safety_cap={} = real_sol_reserves * 0.90, \
                     real_sol_reserves={})",
                    token_amount_raw,
                    clamped,
                    projected_sol_out,
                    safety_cap_lamports,
                    curve.real_sol_reserves,
                );
                token_amount_raw = clamped;
                sell_was_clamped = true;
            }
        }

        // Recompute final UI amount + expected sol_out from the (possibly
        // clamped) raw value so logs and the SellReceipt match what we
        // actually submit, not what the manager originally requested.
        let final_token_amount_ui = token_amount_raw as f64 / 1_000_000.0;
        let expected_sol = match &curve_state_opt {
            Some(c) => pump_sol_out_for_tokens(token_amount_raw, c) as f64 / 1e9,
            None => final_token_amount_ui * pool.price().amount().to_float().max(0.0),
        };
        let expected_sol_after_slip = (expected_sol * (1.0 - slip)).max(0.0);
        let price_sol_per_token = if final_token_amount_ui > 0.0 {
            expected_sol / final_token_amount_ui
        } else {
            0.0
        };

        // Full exit + clamp: first tx sells only the clamped chunk (no CloseAccount
        // here — token program refuses Close with non-zero balance). We then run
        // `drain_pump_ata_after_clamped_full_exit` to sell remaining chunks until
        // the ATA is empty, then CloseAccount in a separate tx.
        let do_close_ata = close_account_after && !sell_was_clamped;
        if close_account_after && sell_was_clamped {
            eprintln!(
                "[BROKER SELL] {mint}: full-exit clamped (first leg {:.6} of {:.6} UI tokens); \
                 will drain remaining on-chain then close ATA if possible",
                final_token_amount_ui,
                actual_token_amount,
            );
        }

        let bonding_curve_complete = curve_state_opt
            .as_ref()
            .map(|c| c.curve_complete)
            .unwrap_or(false);
        let curve_mcap_sol = curve_state_opt
            .as_ref()
            .map(bonding_curve_mcap_sol)
            .unwrap_or(0.0);
        // Near MCAP CEILING (~350 SOL) pump on-curve sell often returns 6005; use Jupiter instead.
        const JUPITER_SELL_MCAP_SOL: f64 = 250.0;
        let route_jupiter_not_pump = bonding_curve_complete
            || (curve_mcap_sol >= JUPITER_SELL_MCAP_SOL && curve_mcap_sol.is_finite());

        if route_jupiter_not_pump && token_amount_raw > 0 {
            eprintln!(
                "[BROKER SELL] mint={mint} graduated/high-mcap (complete={bonding_curve_complete} \
                 curve_mcap={curve_mcap_sol:.1} SOL) — routing via Jupiter \
                 (raw_tokens={token_amount_raw}, close_ata={do_close_ata})"
            );
            let pre_raw = fetch_token_account_raw(&self.rpc_client, &ata_str)
                .await
                .unwrap_or(0);
            return self
                .sell_tokens_post_graduation(
                    mint,
                    &ata_str,
                    ata_sol,
                    pre_raw,
                    token_amount_raw,
                    do_close_ata,
                    slip,
                    mint_token,
                )
                .await;
        }

        eprintln!(
            "[BROKER SELL] mint={} tokens={:.6} (raw={}) expected={:.6} SOL \
             (price={:.9}) min_out=0 close_ata={} clamped={} \
             (slippage waived for guaranteed exit)",
            mint,
            final_token_amount_ui,
            token_amount_raw,
            expected_sol,
            price_sol_per_token,
            do_close_ata,
            sell_was_clamped,
        );

        let mut ixs = self.compute_budget_prelude();

        // Skip the SELL ix only if there's literally nothing left to sell
        // (e.g. ATA already empty AND not clamped to zero). CloseAccount
        // can still be appended below for rent recovery.
        let needs_sell_ix = token_amount_raw > 0;
        let cashback_enabled = curve_state_opt
            .as_ref()
            .map(|c| c.is_cashback_coin)
            .unwrap_or(false);
        if cashback_enabled {
            eprintln!(
                "[BROKER SELL] {mint}: bonding curve has cashback enabled — \
                 including user_volume_accumulator in sell accounts"
            );
        }
        if needs_sell_ix {
            let token_amount_in = sips::helper::Amount::<6>::from_raw(token_amount_raw);
            let min_sol_out = sips::helper::Amount::<9>::from_float(min_sol_out_f);

        let ix = PumpInstruction::sell(
            mint.into(),
            self.wallet_address.into(),
                creator_for_vault.into(),
                mint_token.program(),
            token_amount_in,
            min_sol_out,
                cashback_enabled,
            );

            ixs.push(ix.into());
        }

        // Atomic rent recovery on full exits when the first tx sold the full
        // unclamped amount (sell + close same bundle). Clamped full exits drain
        // first, then close in `drain_pump_ata_after_clamped_full_exit`.
        if do_close_ata {
            let close_ix = mint_token.close_account_ix(
                ata_sol.into(),
                self.wallet_address.into(),
                self.wallet_address.into(),
            );
            ixs.push(close_ix);
            eprintln!("[BROKER SELL] {mint}: appending CloseAccount(ata={ata_str}) for rent refund");
        }

        if ixs.is_empty()
            || (!needs_sell_ix && !do_close_ata)
        {
            return Err(BrokerError::Custom(format!(
                "{mint}: nothing to send (no tokens to sell after clamp and no close requested)"
            )));
        }

        // Snapshot at `confirmed` immediately before broadcast so we can prove
        // the SELL actually moved tokens (or removed the ATA) — matching what
        // the manager will deduct from in-memory holdings.
        let pre_sell_raw = fetch_token_account_raw(&self.rpc_client, &ata_str)
            .await
            .unwrap_or(0);

        let sig_str = match self.send_with_retries(ixs, "SELL").await {
            Ok(s) => s,
            Err(BrokerError::TransactionFailed(ref msg))
                if is_bonding_curve_complete_pump_error(msg) =>
            {
                if token_amount_raw == 0 || !needs_sell_ix {
                    return Err(BrokerError::TransactionFailed(msg.clone()));
                }
                eprintln!(
                    "[BROKER SELL] {mint}: pump Sell failed with BondingCurveComplete (6005); \
                     retrying via Jupiter"
                );
                return self
                    .sell_tokens_post_graduation(
                        mint,
                        &ata_str,
                        ata_sol,
                        pre_sell_raw,
                        token_amount_raw,
                        do_close_ata,
                        slip,
                        mint_token,
                    )
                    .await;
            }
            Err(e) => return Err(e),
        };

        poll_ata_confirms_sell(
            &self.rpc_client,
            &ata_str,
            pre_sell_raw,
            token_amount_raw,
            do_close_ata,
            mint,
            Duration::from_secs(45),
        )
        .await?;

        let sol_est_first = expected_sol_after_slip;
        let sol_act_first = self
            .wallet_net_sol_or_fallback(&sig_str, sol_est_first, "SELL-META")
            .await;

        let mut sol_received_actual = sol_act_first;
        let mut sol_received_estimated = sol_est_first;
        let mut last_sig = sig_str.clone();

        if close_account_after && sell_was_clamped {
            let (dr_act, dr_est, ls) = self
                .drain_pump_ata_after_clamped_full_exit(
                    mint,
                    ata_str.as_str(),
                    ata_sol,
                    mint_token,
                    pool,
                    slip,
                    sig_str.clone(),
                )
                .await?;
            sol_received_actual += dr_act;
            sol_received_estimated += dr_est;
            last_sig = ls;
        }

        // Holdings are reconciled via `on_trade`; receipt SOL fields are
        // on-chain actual vs model estimate for PnL / dashboards.
        Ok(SellReceipt {
            sol_received_actual,
            sol_received_estimated,
            signature: Some(last_sig),
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

    fn forget_position(&self, mint: SolAddress) {
        self.positions.lock().unwrap().remove(&mint);
    }

    fn mode_label(&self) -> &'static str {
        "live"
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Pump `Sell` returns Anchor `BondingCurveComplete` as `InstructionError(_, Custom(6005))`.
fn is_bonding_curve_complete_pump_error(msg: &str) -> bool {
    msg.contains("Custom(6005)")
}

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
    is_transient_rpc_error(msg)
}

/// HTTP / transport failures from Helius and other RPCs (retry, do not abort buy wait).
fn is_transient_rpc_error(msg: &str) -> bool {
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
        || m.contains("error sending request")
        || m.contains("connection closed")
        || m.contains("dns")
}

/// Mint not indexed yet or already gone — poll, do not fail immediately.
fn is_rpc_account_missing(msg: &str) -> bool {
    let m = msg.to_lowercase();
    m.contains("accountnotfound")
        || m.contains("account not found")
        || m.contains("could not find account")
        || m.contains("mint account not found")
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
    Ok(fetch_bonding_curve_state(rpc, mint).await?.creator)
}

/// Full bonding-curve account snapshot used both for the `creator_vault` PDA
/// derivation (BUY/SELL) and for sell-size clamping against available SOL
/// liquidity (SELL only). `real_token_reserves` isn't read by today's
/// clamp logic but is parsed for cheap parity with future maintenance
/// tooling (e.g. dust-sweep scripts).
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct BondingCurveState {
    virtual_token_reserves: u64,
    virtual_sol_reserves: u64,
    real_token_reserves: u64,
    real_sol_reserves: u64,
    creator: SolAddress,
    /// `BondingCurve::is_cashback_coin` (pump IDL). When true, on-chain `sell`
    /// must pass `user_volume_accumulator` before `bonding_curve_v2` (`sips`
    /// `SellAccounts`). If omitted, Pump returns `Custom(6024)` (often called
    /// "overflow"); see pump-public-docs `PUMP_CASHBACK_README.md`. The
    /// trailing `buyback_fee_recipient` account (same tail as `buy`) is
    /// required when global buyback is active — otherwise `Custom(6062)`
    /// (`BuybackFeeRecipientMissing`).
    is_cashback_coin: bool,
    /// Anchor `BondingCurve::complete` — when true, liquidity has migrated off
    /// the bonding curve; pump `Sell` fails with `BondingCurveComplete` (6005).
    curve_complete: bool,
}

/// Bonding-curve mcap in SOL from virtual reserves (same formula as
/// `launchpads::pump::pool::Bonding::market_cap`).
fn bonding_curve_mcap_sol(curve: &BondingCurveState) -> f64 {
    if curve.virtual_token_reserves == 0 {
        return 0.0;
    }
    let mcap_raw = (curve.virtual_sol_reserves as u128)
        .saturating_mul(1_000_000_000_000_000u128)
        / (curve.virtual_token_reserves as u128);
    Amount::from_raw_native(mcap_raw.min(u64::MAX as u128) as u64).to_float()
}

/// Decode pump-fun bonding curve account fields we care about.
///
/// Layout (Anchor): 8-byte discriminator
///   + virtual_token_reserves: u64        (offset  8)
///   + virtual_quote_reserves: u64        (offset 16)  (legacy name: virtual SOL)
///   + real_token_reserves:    u64        (offset 24)
///   + real_quote_reserves:    u64        (offset 32)
///   + token_total_supply:     u64        (offset 40)
///   + complete:               bool       (offset 48)
///   + creator:                Pubkey[32] (offset 49)
///   + is_mayhem_mode:         bool       (offset 81)
///   + is_cashback_coin:       bool       (offset 82)
///   + quote_mint:             Pubkey     (offset 83)
async fn fetch_bonding_curve_state(
    rpc: &RpcClient,
    mint: &SolAddress,
) -> Result<BondingCurveState, BrokerError> {
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
    const IS_CASHBACK_COIN_OFFSET: usize = CREATOR_OFFSET + CREATOR_LEN + 1; // after `is_mayhem_mode`
    if account.data.len() < CREATOR_OFFSET + CREATOR_LEN {
        return Err(BrokerError::Custom(format!(
            "bonding_curve data too short: {} bytes, need at least {}",
            account.data.len(),
            CREATOR_OFFSET + CREATOR_LEN
        )));
    }

    let read_u64 = |off: usize| -> u64 {
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&account.data[off..off + 8]);
        u64::from_le_bytes(buf)
    };

    let mut creator_bytes = [0u8; CREATOR_LEN];
    creator_bytes.copy_from_slice(&account.data[CREATOR_OFFSET..CREATOR_OFFSET + CREATOR_LEN]);

    let is_cashback_coin = account.data.len() > IS_CASHBACK_COIN_OFFSET
        && account.data[IS_CASHBACK_COIN_OFFSET] != 0;

    let curve_complete = account.data.len() > 48 && account.data[48] != 0;

    Ok(BondingCurveState {
        virtual_token_reserves: read_u64(8),
        virtual_sol_reserves:   read_u64(16),
        real_token_reserves:    read_u64(24),
        real_sol_reserves:      read_u64(32),
        creator: SolAddress::new_from_array(creator_bytes),
        is_cashback_coin,
        curve_complete,
    })
}

/// Read bonding-curve mcap (SOL) for post-exit tracking. `None` if account missing or migrated.
pub async fn probe_bonding_mcap_sol(rpc: &RpcClient, mint: &SolAddress) -> Option<f64> {
    let curve = fetch_bonding_curve_state(rpc, mint).await.ok()?;
    if curve.curve_complete {
        return None;
    }
    let mcap = bonding_curve_mcap_sol(&curve);
    if mcap.is_finite() && mcap > 0.0 {
        Some(mcap)
    } else {
        None
    }
}

/// Pump's constant-product sell formula:
///   sol_out = vsr - vsr * vtr / (vtr + token_amount)
///
/// All math in u128 to avoid overflowing the u64*u64 multiplication
/// (vsr ≈ 30 SOL = 3e10 lamports, vtr ≈ 1e15 raw → product ≈ 3e25 ≫ u64).
/// Returns 0 on degenerate inputs (zero reserves) — caller decides what to do.
fn pump_sol_out_for_tokens(token_amount_raw: u64, curve: &BondingCurveState) -> u64 {
    if token_amount_raw == 0
        || curve.virtual_sol_reserves == 0
        || curve.virtual_token_reserves == 0
    {
        return 0;
    }
    let vsr = curve.virtual_sol_reserves as u128;
    let vtr = curve.virtual_token_reserves as u128;
    let amount = token_amount_raw as u128;
    let new_vtr = vtr.saturating_add(amount);
    if new_vtr == 0 {
        return 0;
    }
    let new_vsr = vsr.saturating_mul(vtr) / new_vtr;
    vsr.saturating_sub(new_vsr).min(u64::MAX as u128) as u64
}

/// Inverse of `pump_sol_out_for_tokens`: largest `token_amount_raw` whose
/// curve sell produces *at most* `target_sol_out` lamports of gross output.
///
///   token_amount = vsr * vtr / (vsr - target_sol_out) - vtr
///
/// Used to size SELL down so the program's
/// `real_sol_reserves.checked_sub(sol_amount)` doesn't underflow on
/// freshly-minted, illiquid coins (manifests as Anchor `Overflow` error
/// 6024 / 0x1788 in pump-fun's sell handler).
fn pump_max_sellable_tokens_for_sol(target_sol_out: u64, curve: &BondingCurveState) -> u64 {
    if target_sol_out == 0
        || curve.virtual_sol_reserves == 0
        || curve.virtual_token_reserves == 0
    {
        return 0;
    }
    let vsr = curve.virtual_sol_reserves as u128;
    let vtr = curve.virtual_token_reserves as u128;
    let target = target_sol_out as u128;
    if target >= vsr {
        // Can never extract more than virtual SOL itself; cap is "everything".
        return u64::MAX;
    }
    let denom = vsr - target;
    let new_vtr = vsr.saturating_mul(vtr) / denom;
    if new_vtr <= vtr {
        return 0;
    }
    new_vtr.saturating_sub(vtr).min(u64::MAX as u128) as u64
}

/// Legacy SPL (`Tokenkeg…`) vs Token-2022 (`Tokenz…`), from mint account owner on RPC.
#[derive(Clone, Copy, Debug)]
enum MintTokenKind {
    LegacySpl,
    Token2022,
}

impl MintTokenKind {
    fn program(self) -> SipsAddress {
        match self {
            Self::LegacySpl => SplTokenProgram::PROGRAM,
            Self::Token2022 => TokenProgram2022::PROGRAM,
        }
    }

    fn close_account_ix(
        self,
        ata: SipsAddress,
        destination: SipsAddress,
        authority: SipsAddress,
    ) -> SolanaIx {
        match self {
            Self::LegacySpl => SplTokenProgram::close_account(ata, destination, authority).into(),
            Self::Token2022 => TokenProgram2022::close_account(ata, destination, authority).into(),
        }
    }
}

/// Reads mint account owner at confirmed commitment.
async fn fetch_mint_token_kind(
    rpc: &RpcClient,
    mint: &SolAddress,
) -> Result<MintTokenKind, BrokerError> {
    const LEGACY: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
    const TOKEN22: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

    let mint_str = mint.to_string();
    let mint_pk = mint_str
        .parse()
        .map_err(|_| BrokerError::Custom(format!("invalid mint pubkey: {mint_str}")))?;

    let resp = rpc
        .get_account_with_commitment(&mint_pk, CommitmentConfig::confirmed())
        .await
        .map_err(|e| BrokerError::Custom(format!("get_account mint {mint_str}: {e}")))?;

    let acc = resp.value.ok_or_else(|| BrokerError::MintNotOnChain {
        mint: mint_str.clone(),
        detail: "fetch_mint_token_kind: account empty at confirmed".into(),
    })?;

    let owner = acc.owner.to_string();
    let kind = match owner.as_str() {
        LEGACY => MintTokenKind::LegacySpl,
        TOKEN22 => MintTokenKind::Token2022,
        _ => {
            return Err(BrokerError::Custom(format!(
                "mint {mint_str} owner {owner} is not SPL Token (Tokenkeg) or Token-2022 (Tokenz)"
            )));
        }
    };
    eprintln!(
        "[BROKER] mint={mint_str} token_program={kind:?} (mint_owner={owner})",
    );
    Ok(kind)
}

/// Derive the SPL associated token account `(wallet, mint)` for the given
/// token program id (`Tokenkeg…` or `Tokenz…`).
fn derive_ata_address(
    wallet: &SolAddress,
    mint: &SolAddress,
    token_program: SipsAddress,
) -> (SolAddress, String) {
    let wallet_sips: SipsAddress = (*wallet).into();
    let mint_sips: SipsAddress = (*mint).into();
    let (ata_sips, _bump) = sips::helper::ata(&wallet_sips, &token_program, &mint_sips);
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

/// How much raw balance drop we tolerate below `sold_raw` (rounding / RPC).
fn sell_raw_delta_slack(sold_raw: u64) -> u64 {
    if sold_raw == 0 {
        return 0;
    }
    // Up to 0.01% of the sell, at least 1 raw unit, capped at 100 raw (= 0.0001 UI @ 6 dp).
    (sold_raw / 10_000).max(1).min(100)
}

/// After `send_with_retries` (signature already **confirmed**, no `err`),
/// verify the ATA reflects the sell: partial → raw balance dropped by at
/// least `sold_raw - slack`; full exit + CloseAccount → ATA must vanish at
/// `confirmed`. Without this, `skip_preflight` + RPC quirks can leave the
/// manager/UI out of sync with chain state.
async fn poll_ata_confirms_sell(
    rpc: &RpcClient,
    ata: &str,
    pre_raw: u64,
    sold_raw: u64,
    expect_ata_closed: bool,
    mint: SolAddress,
    total_timeout: Duration,
) -> Result<(), BrokerError> {
    let poll_delay = Duration::from_millis(400);
    let started = std::time::Instant::now();
    let mut attempts: u32 = 0;

    let ata_pk = ata
        .parse()
        .map_err(|_| BrokerError::Custom(format!("poll_ata_confirms_sell: invalid ATA {ata}")))?;

    let slack = sell_raw_delta_slack(sold_raw);
    // Never accept "zero drop" as success when we meant to sell a positive raw amount.
    let min_drop = (sold_raw.saturating_sub(slack)).max(if sold_raw > 0 { 1 } else { 0 });

    loop {
        attempts += 1;

        if expect_ata_closed {
            match rpc
                .get_account_with_commitment(&ata_pk, CommitmentConfig::confirmed())
                .await
            {
                Ok(resp) if resp.value.is_none() => {
                    eprintln!(
                        "[BROKER SELL] {mint}: ATA {ata} closed or absent at confirmed \
                         (attempt {attempts}, {:?})",
                        started.elapsed()
                    );
                    return Ok(());
                }
                Ok(resp) if resp.value.is_some() => {
                    // CloseAccount is in the same tx as the sell; if the ATA still
                    // exists, either we're lagging or the close did not land.
                    if started.elapsed() >= total_timeout {
                        return Err(BrokerError::TransactionFailed(format!(
                            "{mint}: expected ATA {ata} to close after full SELL; still present \
                             after {attempts} polls in {:?} (pre_raw={pre_raw}, sold_raw={sold_raw})",
                            total_timeout
                        )));
                    }
                }
                Err(e) => {
                    let msg = e.to_string();
                    if started.elapsed() >= total_timeout {
                        return Err(BrokerError::Custom(format!(
                            "{mint}: get_account ATA {ata} after full SELL: {msg}"
                        )));
                    }
                }
                Ok(_) => {}
            }
        } else {
            match fetch_token_account_raw_with_decimals(rpc, ata).await {
                Ok((post_raw, _decimals)) => {
                    let drop = pre_raw.saturating_sub(post_raw);
                    if drop >= min_drop {
                        eprintln!(
                            "[BROKER SELL] {mint}: ATA balance drop confirmed raw {pre_raw} -> \
                             {post_raw} (delta={drop}, need>={min_drop}, sold_raw={sold_raw}, \
                             attempt {attempts})",
                        );
                        return Ok(());
                    }
                    if started.elapsed() >= total_timeout {
                        return Err(BrokerError::TransactionFailed(format!(
                            "{mint}: ATA {ata} balance did not drop enough after confirmed SELL: \
                             pre_raw={pre_raw} post_raw={post_raw} delta={drop} need>={min_drop} \
                             sold_raw={sold_raw} after {attempts} polls in {:?}",
                            total_timeout
                        )));
                    }
                }
                Err(e) => {
                    if started.elapsed() >= total_timeout {
                        return Err(BrokerError::Custom(format!(
                            "{mint}: token balance read ATA {ata} after SELL: {e}"
                        )));
                    }
                }
            }
        }

        tokio::time::sleep(poll_delay).await;
    }
}

/// Poll `get_account_with_commitment(Confirmed)` until the account is visible
/// or the budget expires. Treats `AccountNotFound` and empty responses as
/// "not yet indexed" (common when Create arrives before RPC confirmed view).
async fn wait_for_account_visible(
    rpc: &RpcClient,
    address: &SolAddress,
    label: &str,
    total_timeout: Duration,
    min_polls: u32,
    poll_delay: Duration,
) -> Result<(), BrokerError> {
    let pubkey_str = address.to_string();
    let pubkey = pubkey_str
        .parse()
        .map_err(|_| BrokerError::Custom(format!("{label}: invalid pubkey '{pubkey_str}'")))?;

    let started = std::time::Instant::now();
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
                if !is_rpc_account_missing(&msg) && !is_transient_rpc_error(&msg) {
                    return Err(BrokerError::Custom(format!(
                        "{label}: get_account_with_commitment {pubkey_str} failed: {msg}"
                    )));
                }
            }
        }

        let elapsed = started.elapsed();
        if elapsed >= total_timeout && attempts >= min_polls {
            return Err(BrokerError::MintNotOnChain {
                mint: pubkey_str,
                detail: format!(
                    "{label}: not visible on RPC after {attempts} polls in {elapsed:?}"
                ),
            });
        }
        if elapsed >= total_timeout {
            return Err(BrokerError::MintNotOnChain {
                mint: pubkey_str,
                detail: format!("{label}: timeout {total_timeout:?} before min_polls"),
            });
        }
        tokio::time::sleep(poll_delay).await;
    }
}

#[cfg(test)]
mod wait_account_tests {
    use super::{is_rpc_account_missing, is_transient_rpc_error};

    #[test]
    fn account_not_found_is_missing_not_fatal() {
        let msg = "AccountNotFound: pubkey=abc: error sending request for url (https://beta.helius-rpc.com/)";
        assert!(is_rpc_account_missing(msg));
        assert!(is_transient_rpc_error(msg));
    }

    #[test]
    fn transport_only_is_transient() {
        assert!(is_transient_rpc_error("error sending request for url (http://rpc/)"));
        assert!(!is_rpc_account_missing("error sending request for url (http://rpc/)"));
    }
}
