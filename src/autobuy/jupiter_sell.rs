//! Jupiter Swap API v1 (`api.jup.ag/swap/v1`) — trades after pump.fun bonding
//! curve completion (Anchor `BondingCurveComplete` / custom `6005`), when
//! on-curve `buy`/`sell` is rejected.
//!
//! Legacy `quote-api.jup.ag/v6` is deprecated / may not resolve; see Jupiter
//! portal migration docs. Optional env `JUPITER_API_KEY` sets `x-api-key` for
//! higher rate limits.

use std::sync::OnceLock;

use reqwest::Client;
use serde_json::{json, Value};
use solana_transaction::versioned::VersionedTransaction;

use super::broker::BrokerError;

const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
/// Jupiter migrated off `quote-api.jup.ag` (often fails DNS / deprecated). Use Swap API v1.
/// Docs: https://developers.jup.ag/docs/swap/get-quote
const JUPITER_QUOTE: &str = "https://api.jup.ag/swap/v1/quote";
const JUPITER_SWAP: &str = "https://api.jup.ag/swap/v1/swap";

fn http() -> &'static Client {
    static HTTP: OnceLock<Client> = OnceLock::new();
    HTTP.get_or_init(|| {
        Client::builder()
            .timeout(std::time::Duration::from_secs(45))
            .build()
            .expect("jupiter http client")
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
}

/// Generic ExactIn quote + swap (input/output mints are caller-defined).
pub(crate) async fn jupiter_build_swap_exact_in_mints(
    input_mint: &str,
    output_mint: &str,
    amount_raw: u64,
    slippage_bps: u16,
    user_pubkey: &str,
) -> Result<JupiterSwapBuild, BrokerError> {
    let h = http();
    let quote_url = format!(
        "{JUPITER_QUOTE}?inputMint={input_mint}&outputMint={output_mint}\
         &amount={amount_raw}&slippageBps={slippage_bps}&swapMode=ExactIn"
    );

    let quote: Value = jupiter_request(h, reqwest::Method::GET, &quote_url)
        .send()
        .await
        .map_err(|e| BrokerError::Custom(format!("Jupiter quote request: {e}")))?
        .error_for_status()
        .map_err(|e| BrokerError::Custom(format!("Jupiter quote HTTP: {e}")))?
        .json()
        .await
        .map_err(|e| BrokerError::Custom(format!("Jupiter quote JSON: {e}")))?;

    if let Some(err) = quote.get("error") {
        return Err(BrokerError::Custom(format!("Jupiter quote error: {err}")));
    }

    let out_lamports = quote
        .get("outAmount")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);

    let body = json!({
        "quoteResponse": quote,
        "userPublicKey": user_pubkey,
        "wrapAndUnwrapSol": true,
        "dynamicComputeUnitLimit": true,
        "prioritizationFeeLamports": "auto",
    });

    let swap: Value = jupiter_request(h, reqwest::Method::POST, JUPITER_SWAP)
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await
        .map_err(|e| BrokerError::Custom(format!("Jupiter swap request: {e}")))?
        .error_for_status()
        .map_err(|e| BrokerError::Custom(format!("Jupiter swap HTTP: {e}")))?
        .json()
        .await
        .map_err(|e| BrokerError::Custom(format!("Jupiter swap JSON: {e}")))?;

    let swap_transaction_b64 = swap
        .get("swapTransaction")
        .and_then(|v| v.as_str())
        .ok_or_else(|| BrokerError::Custom("Jupiter swap: missing swapTransaction".into()))?
        .to_string();

    Ok(JupiterSwapBuild {
        swap_transaction_b64,
        out_lamports: out_lamports,
    })
}

/// Sell path: token mint → wrapped SOL.
pub(crate) async fn jupiter_build_swap_exact_in(
    input_mint: &str,
    amount_raw: u64,
    slippage_bps: u16,
    user_pubkey: &str,
) -> Result<JupiterSwapBuild, BrokerError> {
    jupiter_build_swap_exact_in_mints(input_mint, WSOL_MINT, amount_raw, slippage_bps, user_pubkey)
        .await
}

/// Buy path after graduation: wrapped SOL → token mint (`bondingCurveComplete` / 6005 on pump buy).
pub(crate) async fn jupiter_build_swap_wsol_to_mint_exact_in(
    output_mint: &str,
    sol_lamports_in: u64,
    slippage_bps: u16,
    user_pubkey: &str,
) -> Result<JupiterSwapBuild, BrokerError> {
    jupiter_build_swap_exact_in_mints(
        WSOL_MINT,
        output_mint,
        sol_lamports_in,
        slippage_bps,
        user_pubkey,
    )
    .await
}
