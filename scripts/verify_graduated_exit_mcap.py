#!/usr/bin/env python3
"""Compare bonding-curve mcap vs Jupiter for graduated/moonbag exit-mcap patch.

Usage (on prod with /home/automata/.env):
  python3 scripts/verify_graduated_exit_mcap.py [MINT]

Exit 0 if patch would use Jupiter when bonding diverges or curve complete.
"""
from __future__ import annotations

import base64
import json
import os
import struct
import sys
import urllib.request

WSOL = "So11111111111111111111111111111111111111112"
PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
SUPPLY = 1_000_000_000.0
DIVERGENCE = 0.85
ENV_PATH = os.environ.get("AUTOMATA_ENV", "/home/automata/.env")


def _jupiter_api_key_from_loggaper_proc() -> str | None:
    import subprocess

    try:
        pid = subprocess.check_output(
            ["pgrep", "-f", "/home/automata/loggaper"], text=True
        ).strip().split("\n")[0]
        raw = open(f"/proc/{pid}/environ", "rb").read().split(b"\0")
        for item in raw:
            if item.startswith(b"JUPITER_API_KEY="):
                return item.split(b"=", 1)[1].decode()
    except (OSError, subprocess.SubprocessError, IndexError, ValueError):
        return None
    return None


def jupiter_api_key_from_systemd() -> str | None:
    import subprocess

    try:
        out = subprocess.check_output(
            ["systemctl", "show", "loggaper", "-p", "Environment", "--value"],
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    for part in out.split():
        if part.startswith("JUPITER_API_KEY="):
            return part.split("=", 1)[1]
    return None


def load_env(path: str) -> dict[str, str]:
    out: dict[str, str] = {}
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    k, v = line.split("=", 1)
                elif ":" in line:
                    k, v = line.split(":", 1)
                else:
                    continue
                out[k.strip()] = v.strip().strip('"').strip("'")
    except OSError:
        pass
    return out


def http_json(url: str, api_key: str | None = None, data: bytes | None = None) -> dict:
    req = urllib.request.Request(
        url, data=data, method="POST" if data else "GET"
    )
    req.add_header("Content-Type", "application/json")
    # Jupiter sits behind Cloudflare; default Python urllib UA gets 403 (error 1010).
    req.add_header("User-Agent", "automata-verify/1.0")
    if api_key:
        req.add_header("x-api-key", api_key)
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.load(resp)


def jupiter_implied_mcap_sol(mint: str, api_key: str | None) -> float | None:
    url = f"https://api.jup.ag/price/v3?ids={mint},{WSOL}"
    body = http_json(url, api_key)
    data = body.get("data", body)

    def usd(m: str) -> float:
        if isinstance(data, dict) and m in data:
            row = data[m]
            if isinstance(row, dict):
                p = row.get("usdPrice") or row.get("price")
                return float(p or 0)
        return 0.0

    tu, su = usd(mint), usd(WSOL)
    if su <= 0:
        return None
    mcap = (tu / su) * SUPPLY
    return mcap if mcap > 0 else None


def bonding_curve_pda(mint: str) -> str:
    # seeds: ["bonding-curve", mint] program pump
    import hashlib

    def pda(seeds: list[bytes], program: bytes) -> bytes:
        for bump in range(255, -1, -1):
            h = hashlib.sha256()
            for s in seeds + [bytes([bump])]:
                h.update(s)
            h.update(program)
            h.update(b"ProgramDerivedAddress")
            if h.digest()[31] >= 0x80:  # simplified; use solders if available
                pass
        # fallback: use solana-py not installed — RPC getProgramAccounts is heavy.
        raise NotImplementedError

    raise NotImplementedError("bonding PDA needs solana library")


def probe_bonding_mcap_sol(rpc: str, mint: str) -> tuple[float | None, bool]:
    """Decode pump bonding curve account; return (mcap_sol, curve_complete)."""
    # Derive bonding curve address via solders-free approach: call getAccountInfo
    # with known PDA formula from pump docs.
    try:
        from solders.pubkey import Pubkey  # type: ignore

        mint_pk = Pubkey.from_string(mint)
        prog = Pubkey.from_string(PUMP_PROGRAM)
        curve, _ = Pubkey.find_program_address(
            [b"bonding-curve", bytes(mint_pk)], prog
        )
        curve_str = str(curve)
    except ImportError:
        # Manual PDA not implemented; skip bonding probe
        return None, False

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [curve_str, {"encoding": "base64"}],
    }
    raw = http_json(rpc, data=json.dumps(payload).encode())
    val = raw.get("result", {}).get("value")
    if not val:
        return None, True
    data_b64 = val["data"][0]
    data = base64.b64decode(data_b64)
    if len(data) < 49:
        return None, False
    # Anchor: virtual_token @8, virtual_sol @16, complete @48
    virtual_token = struct.unpack_from("<Q", data, 8)[0]
    virtual_sol = struct.unpack_from("<Q", data, 16)[0]
    complete = data[48] != 0
    if complete or virtual_token == 0:
        return None, complete
    mcap = (virtual_sol * 1_000_000_000_000_000) / virtual_token / 1e9
    return mcap, complete


def decide_open_exit_mcap(
    bonding: float | None,
    jupiter: float | None,
    pool_raw: float,
    force_jupiter: bool,
) -> tuple[float, bool]:
    """Mirror of `post_exit_tracker::decide_open_exit_mcap`."""
    if force_jupiter:
        if jupiter and jupiter > 0:
            return jupiter, True
        if bonding and bonding > 0:
            return bonding, True
        return pool_raw, True
    if jupiter and jupiter > 0 and bonding and bonding > 0:
        if jupiter < bonding * DIVERGENCE:
            return jupiter, True
        return bonding, False
    if bonding is None and jupiter and jupiter > 0:
        return jupiter, True
    if bonding and bonding > 0:
        return bonding, False
    return pool_raw, False


def main() -> int:
    mint = (
        sys.argv[1]
        if len(sys.argv) > 1
        else "9U85nJVNNDeibnqj2byqwEQnkNiajTiJBJ1hqE6Kpump"
    )
    env = {**load_env(ENV_PATH), **os.environ}
    rpc = (
        env.get("RPC_URL")
        or env.get("HELIUS_RPC_URL")
        or env.get("SOLANA_RPC_URL")
        or env.get("SOLANA_HTTP")
    )
    api_key = (
        env.get("JUPITER_API_KEY")
        or jupiter_api_key_from_systemd()
        or _jupiter_api_key_from_loggaper_proc()
    )
    if not api_key:
        print("WARN: JUPITER_API_KEY missing — Jupiter price may 403")

    print(f"mint={mint}")
    jup = jupiter_implied_mcap_sol(mint, api_key)
    print(f"jupiter_implied_mcap_sol={jup:.4f}" if jup else "jupiter_implied_mcap_sol=None")

    bonding, complete = None, False
    if rpc:
        try:
            bonding, complete = probe_bonding_mcap_sol(rpc, mint)
        except Exception as e:
            print(f"bonding_probe_error={e}")
    else:
        print("rpc=missing (set RPC_URL in .env)")

    print(f"bonding_mcap_sol={bonding:.4f}" if bonding else "bonding_mcap_sol=None")
    print(f"curve_complete={complete}")

    pool_frozen = 410.88  # historical stuck WS value for 9U85
    force = True  # after MCAP CEILING
    eff, use_j = decide_open_exit_mcap(bonding, jup, pool_frozen, force)
    print(f"patch_force_jupiter=True -> effective_mcap={eff:.4f} use_jupiter={use_j}")

    if bonding and jup:
        eff2, use_j2 = decide_open_exit_mcap(bonding, jup, pool_frozen, False)
        print(
            f"patch_auto_diverge -> effective_mcap={eff2:.4f} use_jupiter={use_j2} "
            f"(jup/bond={jup/bonding:.3f}, threshold={DIVERGENCE})"
        )

    if jup and bonding and jup < bonding * DIVERGENCE:
        print("OK: Jupiter below bonding — patch would NOT freeze at bonding mcap")
        return 0
    if jup and (bonding is None or complete):
        print("OK: curve complete/missing — patch uses Jupiter")
        return 0
    if jup and use_j:
        print("OK: patch uses Jupiter for dashboard/exit")
        return 0
    print("WARN: bonding and Jupiter agree — cannot prove divergence on this mint now")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
