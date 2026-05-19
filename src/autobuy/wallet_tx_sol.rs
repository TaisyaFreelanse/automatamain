//! Resolve net SOL credited to our wallet from a confirmed transaction using
//! RPC `getTransaction` JSON encoding and `meta.pre_balances` /
//! `meta.post_balances` (full account list = static keys + loaded addresses).

use std::time::Duration;

use solana_address::Address as SolAddress;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_keypair::Signature;
use solana_rpc_client_types::config::{CommitmentConfig, CommitmentLevel, RpcTransactionConfig};
use solana_transaction_status_client_types::{
    option_serializer::OptionSerializer, EncodedTransaction, UiMessage, UiTransactionStatusMeta,
};

use super::broker::BrokerError;

const META_FETCH_RETRIES: u32 = 10;
const META_FETCH_DELAY_MS: u64 = 200;

/// Builds the account key list aligned with `pre_balances` / `post_balances`.
fn account_keys_for_meta(
    encoded_tx: &EncodedTransaction,
    meta: &UiTransactionStatusMeta,
) -> Result<Vec<String>, BrokerError> {
    let mut keys = match encoded_tx {
        EncodedTransaction::Json(ui) => match &ui.message {
            UiMessage::Parsed(pm) => pm
                .account_keys
                .iter()
                .map(|a| a.pubkey.clone())
                .collect::<Vec<_>>(),
            UiMessage::Raw(rm) => rm.account_keys.clone(),
        },
        EncodedTransaction::LegacyBinary(_)
        | EncodedTransaction::Binary(_, _)
        | EncodedTransaction::Accounts(_) => {
            return Err(BrokerError::Custom(
                "getTransaction: expected JSON-encoded transaction (set encoding=json)"
                    .into(),
            ));
        }
    };

    if let OptionSerializer::Some(loaded) = &meta.loaded_addresses {
        keys.extend(loaded.writable.iter().cloned());
        keys.extend(loaded.readonly.iter().cloned());
    }

    Ok(keys)
}

fn wallet_lamport_delta(
    meta: &UiTransactionStatusMeta,
    keys: &[String],
    wallet: &str,
) -> Result<i128, BrokerError> {
    let idx = keys.iter().position(|k| k == wallet).ok_or_else(|| {
        BrokerError::Custom(format!(
            "wallet {wallet} not present in transaction account keys (len={})",
            keys.len()
        ))
    })?;
    let pre = *meta.pre_balances.get(idx).unwrap_or(&0) as i128;
    let post = *meta.post_balances.get(idx).unwrap_or(&0) as i128;
    Ok(post - pre)
}

async fn try_wallet_net_sol_once(
    rpc: &RpcClient,
    sig_str: &str,
    wallet: &SolAddress,
) -> Result<f64, BrokerError> {
    let sig: Signature = sig_str.parse().map_err(|_| {
        BrokerError::Custom(format!("wallet_tx_sol: invalid signature base58: {sig_str}"))
    })?;

    let cfg = RpcTransactionConfig {
        encoding: Some(solana_transaction_status_client_types::UiTransactionEncoding::Json),
        commitment: Some(CommitmentConfig {
            commitment: CommitmentLevel::Confirmed,
        }),
        max_supported_transaction_version: Some(0),
    };

    let confirmed = rpc
        .get_transaction_with_config(&sig, cfg)
        .await
        .map_err(|e| BrokerError::Custom(format!("getTransaction {sig_str}: {e}")))?;

    let enc_with_meta = &confirmed.transaction;
    let meta = enc_with_meta
        .meta
        .as_ref()
        .ok_or_else(|| BrokerError::Custom(format!("getTransaction {sig_str}: missing meta")))?;

    if meta.err.is_some() {
        return Err(BrokerError::Custom(format!(
            "getTransaction {sig_str}: transaction meta has err {:?}",
            meta.err
        )));
    }

    let keys = account_keys_for_meta(&enc_with_meta.transaction, meta)?;
    if keys.len() != meta.pre_balances.len() || keys.len() != meta.post_balances.len() {
        return Err(BrokerError::Custom(format!(
            "getTransaction {sig_str}: balance/key len mismatch keys={} pre={} post={}",
            keys.len(),
            meta.pre_balances.len(),
            meta.post_balances.len()
        )));
    }

    let wallet_s = wallet.to_string();
    let delta_lamports = wallet_lamport_delta(meta, &keys, &wallet_s)?;

    Ok(delta_lamports as f64 / 1_000_000_000.0)
}

/// Net SOL received by `wallet` in transaction `sig_str` (lamport delta / 1e9).
pub async fn wallet_net_sol_received_f64(
    rpc: &RpcClient,
    sig_str: &str,
    wallet: &SolAddress,
) -> Result<f64, BrokerError> {
    let mut last_err = BrokerError::Custom("wallet_net_sol_received_f64: no attempts".into());
    for attempt in 1..=META_FETCH_RETRIES {
        match try_wallet_net_sol_once(rpc, sig_str, wallet).await {
            Ok(v) => return Ok(v),
            Err(e) => {
                last_err = e;
                if attempt < META_FETCH_RETRIES {
                    tokio::time::sleep(Duration::from_millis(META_FETCH_DELAY_MS)).await;
                }
            }
        }
    }
    Err(last_err)
}
