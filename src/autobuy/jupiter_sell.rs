//! Jupiter Swap API v1 (`api.jup.ag/swap/v1`) — trades after pump.fun bonding
//! curve completion (Anchor `BondingCurveComplete` / custom `6005`), when
//! on-curve `buy`/`sell` is rejected.
//!
//! Legacy `quote-api.jup.ag/v6` is deprecated / may not resolve; see Jupiter
//! portal migration docs. Optional env `JUPITER_API_KEY` sets `x-api-key` for
//! higher rate limits.

use std::sync::OnceLock;

use base64::{engine::general_purpose::STANDARD, Engine as _};
use reqwest::Client;
use serde_json::{json, Value};
use solana_keypair::{Keypair, Signature};
use solana_transaction::versioned::VersionedTransaction;

use super::broker::BrokerError;

const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
/// Jupiter migrated off `quote-api.jup.ag` (often fails DNS / deprecated). Use Swap API v1.
/// Docs: https://developers.jup.ag/docs/swap/get-quote
const JUPITER_QUOTE: &str = "https://api.jup.ag/swap/v1/quote";
const JUPITER_SWAP: &str = "https://api.jup.ag/swap/v1/swap";
/// Meta-Aggregator order + execute (JupiterZ / RFQ when Metis has no route).
/// Docs: https://developers.jup.ag/docs/swap/order-and-execute
const JUPITER_ORDER_V2: &str = "https://api.jup.ag/swap/v2/order";
const JUPITER_EXECUTE_V2: &str = "https://api.jup.ag/swap/v2/execute";
/// Price API v3 — USD spot (post-exit mcap fallback when bonding curve is gone).
/// Docs: https://developers.jup.ag/docs/price
const JUPITER_PRICE_V3: &str = "https://api.jup.ag/price/v3";
/// pump.fun total token supply (whole tokens) for implied SOL mcap from spot price.
const PUMP_FUN_TOKEN_SUPPLY: f64 = 1_000_000_000.0;

/// Jupiter labels for on-curve / Pump AMM hops (`InstructionError Custom(6005)` after graduation).
/// See https://lite-api.jup.ag/swap/v1/program-id-to-label — spaces as `+` in query string.
pub(crate) const JUPITER_EXCLUDE_PUMP_DEXES: &str = "Pump.fun,Pump.fun+Amm";

/// Quote options for post-graduation pump mint swaps (never route through bonding curve again).
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct JupiterQuoteOpts {
    pub exclude_dexes: Option<&'static str>,
    pub only_direct_routes: bool,
}

impl JupiterQuoteOpts {
    pub const POST_GRADUATION: Self = Self {
        exclude_dexes: Some(JUPITER_EXCLUDE_PUMP_DEXES),
        only_direct_routes: false,
    };

    pub const POST_GRADUATION_DIRECT_ONLY: Self = Self {
        exclude_dexes: Some(JUPITER_EXCLUDE_PUMP_DEXES),
        only_direct_routes: true,
    };

    /// Fallback when `excludeDexes` yields Metis `NO_ROUTES_FOUND` (may route via Pump; 6005 guarded on send).
    pub const POST_GRADUATION_ALLOW_PUMP: Self = Self {
        exclude_dexes: None,
        only_direct_routes: false,
    };

    pub const POST_GRADUATION_ALLOW_PUMP_DIRECT: Self = Self {
        exclude_dexes: None,
        only_direct_routes: true,
    };
}

/// Metis quote HTTP 400 with `errorCode: NO_ROUTES_FOUND` (or equivalent message).
pub(crate) fn is_jupiter_no_routes_found(err: &BrokerError) -> bool {
    match err {
        BrokerError::Custom(msg) => {
            msg.contains("NO_ROUTES_FOUND") || msg.contains("No routes found")
        }
        _ => false,
    }
}

/// v2 `/order` returned HTTP 200 but no signable `transaction`.
pub(crate) fn is_jupiter_empty_order(err: &BrokerError) -> bool {
    match err {
        BrokerError::Custom(msg) => {
            msg.contains("EMPTY_ORDER") || msg.contains("empty transaction")
        }
        _ => false,
    }
}

/// On-chain or `/execute` slippage (Pump/Jupiter custom 15001, etc.).
pub(crate) fn is_jupiter_slippage(err: &BrokerError) -> bool {
    match err {
        BrokerError::TransactionFailed(msg) => {
            msg.contains("15001")
                || msg.to_ascii_lowercase().contains("slippage tolerance exceeded")
                || msg.to_ascii_lowercase().contains("slippage")
        }
        _ => false,
    }
}

pub(crate) fn is_jupiter_execute_failed(err: &BrokerError) -> bool {
    matches!(err, BrokerError::TransactionFailed(_)) && !is_jupiter_slippage(err)
}

pub(crate) fn log_jupiter_fail(tag: &'static str, mint: &str, raw: u64, detail: &str) {
    eprintln!("[JUPITER] {tag} mint={mint} raw={raw}: {detail}");
}

fn http() -> &'static Client {
    static HTTP: OnceLock<Client> = OnceLock::new();
    HTTP.get_or_init(|| {
        Client::builder()
            .timeout(std::time::Duration::from_secs(45))
            .build()
            .expect("jupiter http client")
    })
}

/// Log-safe quote URL (no API keys in query).
fn jupiter_quote_url_for_log(url: &str) -> String {
    url.split('?').next().unwrap_or(url).to_string()
        + "?"
        + &url
            .split('?')
            .nth(1)
            .map(|q| {
                q.split('&')
                    .filter(|p| !p.to_lowercase().starts_with("api-key"))
                    .collect::<Vec<_>>()
                    .join("&")
            })
            .unwrap_or_default()
}

async fn jupiter_http_json(
    method: reqwest::Method,
    url: &str,
    body: Option<Value>,
    op: &str,
) -> Result<Value, BrokerError> {
    let h = http();
    let mut req = jupiter_request(h, method.clone(), url);
    if let Some(ref b) = body {
        req = req
            .header("Content-Type", "application/json")
            .json(b);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| BrokerError::Custom(format!("Jupiter {op} request: {e}")))?;
    let status = resp.status();
    let body_text = resp
        .text()
        .await
        .unwrap_or_else(|e| format!("<body read error: {e}>"));
    if !status.is_success() {
        let log_url = if op.contains("quote") {
            jupiter_quote_url_for_log(url)
        } else {
            op.to_string()
        };
        eprintln!(
            "[JUPITER] {op} HTTP {status} url={log_url} body={body_text}"
        );
        return Err(BrokerError::Custom(format!(
            "Jupiter {op} HTTP {status}: {body_text}"
        )));
    }
    serde_json::from_str(&body_text).map_err(|e| {
        BrokerError::Custom(format!("Jupiter {op} JSON: {e}; body={body_text}"))
    })
}

fn jupiter_request(
    client: &'static Client,
    method: reqwest::Method,
    url: &str,
) -> reqwest::RequestBuilder {
    let mut b = client.request(method, url).header("Accept", "application/json");
    if let Ok(key) = std::env::var("JUPITER_API_KEY") {
        if !key.trim().is_empty() {
            b = b.header("x-api-key", key.trim());
        }
    }
    b
}

/// Deserialize Jupiter `swapTransaction` payload (raw bytes after base64 decode).
pub(crate) fn decode_jupiter_swap_transaction(
    bytes: &[u8],
) -> Result<VersionedTransaction, BrokerError> {
    bincode::deserialize(bytes).map_err(|e| {
        BrokerError::Custom(format!("Jupiter swapTransaction decode (bincode): {e}"))
    })
}

pub(crate) struct JupiterSwapBuild {
    pub swap_transaction_b64: String,
    /// Output mint's smallest units (WSOL lamports when selling into SOL).
    pub out_lamports: u64,
    /// When set, land via `POST /swap/v2/execute` (from `/swap/v2/order`).
    pub v2_request_id: Option<String>,
    /// Quote used `excludeDexes` for Pump.fun / Pump.fun Amm.
    pub used_exclude_pump_dexes: bool,
}

impl JupiterSwapBuild {
    fn from_v1_swap_response(swap: &Value, out_lamports: u64) -> Result<Self, BrokerError> {
        let swap_transaction_b64 = swap
            .get("swapTransaction")
            .and_then(|v| v.as_str())
            .ok_or_else(|| BrokerError::Custom("Jupiter swap: missing swapTransaction".into()))?
            .to_string();
        Ok(Self {
            swap_transaction_b64,
            out_lamports,
            v2_request_id: None,
            used_exclude_pump_dexes: false,
        })
    }
}

/// Swap API ExactIn quote only (no swap transaction).
pub(crate) async fn jupiter_fetch_quote_exact_in(
    input_mint: &str,
    output_mint: &str,
    amount_raw: u64,
    slippage_bps: u16,
    opts: JupiterQuoteOpts,
) -> Result<Value, BrokerError> {
    let mut quote_url = format!(
        "{JUPITER_QUOTE}?inputMint={input_mint}&outputMint={output_mint}\
         &amount={amount_raw}&slippageBps={slippage_bps}&swapMode=ExactIn"
    );
    if let Some(exclude) = opts.exclude_dexes {
        quote_url.push_str("&excludeDexes=");
        quote_url.push_str(exclude);
    }
    if opts.only_direct_routes {
        quote_url.push_str("&onlyDirectRoutes=true");
    }

    let quote = jupiter_http_json(reqwest::Method::GET, &quote_url, None, "quote").await?;

    if let Some(err) = quote.get("error") {
        if err.as_str() == Some("No routes found")
            || quote
                .get("errorCode")
                .and_then(|v| v.as_str())
                == Some("NO_ROUTES_FOUND")
        {
            log_jupiter_fail(
                "NO_ROUTES",
                input_mint,
                amount_raw,
                &format!("{err}"),
            );
        }
        return Err(BrokerError::Custom(format!("Jupiter quote error: {err}")));
    }
    Ok(quote)
}

fn parse_out_amount_lamports(quote: &Value) -> u64 {
    quote
        .get("outAmount")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0)
}

/// `GET /swap/v2/order` — Meta-Aggregator (Metis + JupiterZ + …).
pub(crate) async fn jupiter_build_swap_v2_order_exact_in(
    input_mint: &str,
    amount_raw: u64,
    slippage_bps: u16,
    taker: &str,
    exclude_dexes: Option<&str>,
) -> Result<JupiterSwapBuild, BrokerError> {
    let mut url = format!(
        "{JUPITER_ORDER_V2}?inputMint={input_mint}&outputMint={WSOL_MINT}\
         &amount={amount_raw}&slippageBps={slippage_bps}&swapMode=ExactIn&taker={taker}"
    );
    if let Some(exclude) = exclude_dexes {
        url.push_str("&excludeDexes=");
        url.push_str(exclude);
    }

    let order = jupiter_http_json(reqwest::Method::GET, &url, None, "order").await?;

    if let Some(err) = order.get("error").and_then(|v| v.as_str()) {
        if !err.is_empty() {
            log_jupiter_fail(
                "EMPTY_ORDER",
                input_mint,
                amount_raw,
                &format!("order logical error: {err}"),
            );
            return Err(BrokerError::Custom(format!("Jupiter EMPTY_ORDER: {err}")));
        }
    }

    let swap_transaction_b64 = order
        .get("transaction")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            let code = order.get("errorCode").map(|v| v.to_string()).unwrap_or_default();
            let msg = order
                .get("errorMessage")
                .or_else(|| order.get("error"))
                .map(|v| v.to_string())
                .unwrap_or_default();
            log_jupiter_fail(
                "EMPTY_ORDER",
                input_mint,
                amount_raw,
                &format!("errorCode={code} message={msg}"),
            );
            BrokerError::Custom(format!(
                "Jupiter EMPTY_ORDER: empty transaction (errorCode={code}, message={msg})"
            ))
        })?
        .to_string();

    let request_id = order
        .get("requestId")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| BrokerError::Custom("Jupiter order: missing requestId".into()))?
        .to_string();

    let out_lamports = parse_out_amount_lamports(&order);

    Ok(JupiterSwapBuild {
        swap_transaction_b64,
        out_lamports,
        v2_request_id: Some(request_id),
        used_exclude_pump_dexes: exclude_dexes.is_some(),
    })
}

/// Sign `/order` tx and submit via `POST /swap/v2/execute` (do not refresh blockhash).
pub(crate) async fn jupiter_execute_v2_order(
    build: &JupiterSwapBuild,
    signer: &Keypair,
    mint_log: &str,
    raw_log: u64,
) -> Result<Signature, BrokerError> {
    let request_id = build.v2_request_id.as_ref().ok_or_else(|| {
        BrokerError::Custom("Jupiter execute: missing v2 requestId".into())
    })?;

    let tx_bytes = STANDARD
        .decode(build.swap_transaction_b64.trim())
        .map_err(|e| BrokerError::Custom(format!("Jupiter order tx base64: {e}")))?;
    let template = decode_jupiter_swap_transaction(&tx_bytes)?;
    let signed = VersionedTransaction::try_new(template.message, &[signer]).map_err(|e| {
        BrokerError::Custom(format!("Jupiter order sign: VersionedTransaction::try_new: {e}"))
    })?;
    let signed_b64 = STANDARD.encode(
        bincode::serialize(&signed)
            .map_err(|e| BrokerError::Custom(format!("Jupiter order tx serialize: {e}")))?,
    );

    let body = json!({
        "signedTransaction": signed_b64,
        "requestId": request_id,
    });
    let resp =
        jupiter_http_json(reqwest::Method::POST, JUPITER_EXECUTE_V2, Some(body), "execute").await?;

    let status = resp.get("status").and_then(|v| v.as_str()).unwrap_or("");
    if status == "Success" {
        let sig_str = resp
            .get("signature")
            .and_then(|v| v.as_str())
            .ok_or_else(|| BrokerError::Custom("Jupiter execute: missing signature".into()))?;
        return sig_str.parse::<Signature>().map_err(|e| {
            BrokerError::Custom(format!("Jupiter execute: bad signature: {e}"))
        });
    }

    let err = resp
        .get("error")
        .or_else(|| resp.get("errorMessage"))
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let code = resp
        .get("code")
        .map(|v| v.to_string())
        .unwrap_or_else(|| "?".into());
    let detail = format!("status={status} code={code}: {err}");
    let tag = if code.contains("15001")
        || err.to_ascii_lowercase().contains("slippage tolerance exceeded")
    {
        "SLIPPAGE"
    } else {
        "EXECUTE_FAILED"
    };
    log_jupiter_fail(tag, mint_log, raw_log, &detail);
    Err(BrokerError::TransactionFailed(format!(
        "Jupiter execute {detail}"
    )))
}

fn parse_price_v3_usd(body: &Value, mint: &str) -> Option<f64> {
    let entry = body.get(mint)?;
    let usd = entry
        .get("usdPrice")
        .and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))?;
    if usd.is_finite() && usd > 0.0 {
        Some(usd)
    } else {
        None
    }
}

fn parse_price_v3_decimals(body: &Value, mint: &str) -> Option<u8> {
    body.get(mint)?
        .get("decimals")
        .and_then(|v| v.as_u64())
        .and_then(|d| u8::try_from(d).ok())
}

/// Implied pump-style mcap in SOL from Jupiter Price API v3 (USD / USD).
pub async fn jupiter_implied_mcap_sol_price_v3(mint: &str) -> Option<f64> {
    let url = format!("{JUPITER_PRICE_V3}?ids={mint},{WSOL_MINT}");
    let body: Value = jupiter_request(http(), reqwest::Method::GET, &url)
        .send()
        .await
        .ok()?
        .error_for_status()
        .ok()?
        .json()
        .await
        .ok()?;
    let token_usd = parse_price_v3_usd(&body, mint)?;
    let sol_usd = parse_price_v3_usd(&body, WSOL_MINT)?;
    if sol_usd <= 0.0 {
        return None;
    }
    let mcap = (token_usd / sol_usd) * PUMP_FUN_TOKEN_SUPPLY;
    if mcap.is_finite() && mcap > 0.0 {
        Some(mcap)
    } else {
        None
    }
}

/// Implied mcap in SOL from a small ExactIn quote (token → WSOL).
pub async fn jupiter_implied_mcap_sol_quote(mint: &str) -> Option<f64> {
    let decimals = jupiter_token_decimals(mint).await.unwrap_or(6);
    let one_token_raw = 10u64.pow(u32::from(decimals.min(12)));
    let quote = jupiter_fetch_quote_exact_in(
        mint,
        WSOL_MINT,
        one_token_raw,
        300,
        JupiterQuoteOpts::default(),
    )
    .await
    .ok()?;
    let out_lamports = quote
        .get("outAmount")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<u64>().ok())?;
    if out_lamports == 0 {
        return None;
    }
    let sol_per_token = (out_lamports as f64) / 1e9;
    let mcap = sol_per_token * PUMP_FUN_TOKEN_SUPPLY;
    if mcap.is_finite() && mcap > 0.0 {
        Some(mcap)
    } else {
        None
    }
}

async fn jupiter_token_decimals(mint: &str) -> Option<u8> {
    let url = format!("{JUPITER_PRICE_V3}?ids={mint}");
    let body: Value = jupiter_request(http(), reqwest::Method::GET, &url)
        .send()
        .await
        .ok()?
        .error_for_status()
        .ok()?
        .json()
        .await
        .ok()?;
    parse_price_v3_decimals(&body, mint)
}

/// Bonding-curve-comparable mcap in SOL for migrated / illiquid-on-curve tokens.
pub async fn jupiter_implied_mcap_sol(mint: &str) -> Option<f64> {
    if let Some(m) = jupiter_implied_mcap_sol_price_v3(mint).await {
        return Some(m);
    }
    jupiter_implied_mcap_sol_quote(mint).await
}

/// Generic ExactIn quote + swap (input/output mints are caller-defined).
pub(crate) async fn jupiter_build_swap_exact_in_mints(
    input_mint: &str,
    output_mint: &str,
    amount_raw: u64,
    slippage_bps: u16,
    user_pubkey: &str,
    opts: JupiterQuoteOpts,
) -> Result<JupiterSwapBuild, BrokerError> {
    let quote =
        jupiter_fetch_quote_exact_in(input_mint, output_mint, amount_raw, slippage_bps, opts).await?;

    let out_lamports = parse_out_amount_lamports(&quote);

    let body = json!({
        "quoteResponse": quote,
        "userPublicKey": user_pubkey,
        "wrapAndUnwrapSol": true,
        "dynamicComputeUnitLimit": true,
        "prioritizationFeeLamports": "auto",
    });

    let swap = jupiter_http_json(reqwest::Method::POST, JUPITER_SWAP, Some(body), "swap").await?;

    let mut build = JupiterSwapBuild::from_v1_swap_response(&swap, out_lamports)?;
    build.used_exclude_pump_dexes = opts.exclude_dexes.is_some();
    Ok(build)
}

/// Sell path: token mint → wrapped SOL.
pub(crate) async fn jupiter_build_swap_exact_in(
    input_mint: &str,
    amount_raw: u64,
    slippage_bps: u16,
    user_pubkey: &str,
    opts: JupiterQuoteOpts,
) -> Result<JupiterSwapBuild, BrokerError> {
    jupiter_build_swap_exact_in_mints(
        input_mint,
        WSOL_MINT,
        amount_raw,
        slippage_bps,
        user_pubkey,
        opts,
    )
    .await
}

/// Buy path after graduation: wrapped SOL → token mint (`bondingCurveComplete` / 6005 on pump buy).
pub(crate) async fn jupiter_build_swap_wsol_to_mint_exact_in(
    output_mint: &str,
    sol_lamports_in: u64,
    slippage_bps: u16,
    user_pubkey: &str,
    opts: JupiterQuoteOpts,
) -> Result<JupiterSwapBuild, BrokerError> {
    jupiter_build_swap_exact_in_mints(
        WSOL_MINT,
        output_mint,
        sol_lamports_in,
        slippage_bps,
        user_pubkey,
        opts,
    )
    .await
}
