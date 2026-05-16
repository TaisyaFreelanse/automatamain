use crate::address::Address;
use crate::helper::ata;
use crate::instructions::account::{AccountMeta, IntoAccountMetaArray};
use crate::instructions::pump::instructions::PumpInstruction;
use ix_macros::Accounts;

/// Global buyback fee recipients (pump docs: `docs/FEE_RECIPIENTS.md`).
/// Legacy `buy` / `sell` must pass one of these when buyback is active; missing
/// account surfaces as `Custom(6062)` (`BuybackFeeRecipientMissing`).
pub(crate) const PUMP_BUYBACK_FEE_RECIPIENTS: [&'static str; 8] = [
    "5YxQFdt3Tr9zJLvkFccqXVUwhdTWJQc1fFg2YPbxvxeD",
    "9M4giFFMxmFGXtc3feFzRai56WbBqehoSeRE5GK7gf7",
    "GXPFM2caqTtQYC2cJ5yJRi9VDkpsYZXzYdwYpGnLmtDL",
    "3BpXnfJaUTiwXnJNe7Ej1rcbzqTTQUvLShZaWazebsVR",
    "5cjcW9wExnJJiqgLjq7DEG75Pm6JBgE1hNv4B2vHXUW6",
    "EHAAiTxcdDwQ3U4bU6YcMsQGaekdzLS3B5SmYo46kJtL",
    "5eHhjP8JaYkz83CWwvGU2uMUXefd3AazWGx4gpcuEEYD",
    "A7hAgCzFw14fejgCp387JUJRMNyz4j89JKnhtKU8piqW",
];

#[derive(Accounts, Debug)]
pub struct CreateAccounts {
    #[signer]
    #[writable]
    pub mint: Address,
    pub mint_authority: Address,

    #[writable]
    // #[seeds = b"bonding_curve" + mint]
    pub bonding_curve: Address,

    #[writable]
    pub associated_bonding_curve: Address,

    pub global: Address,
    pub metaplex_token_metadata_program: Address,

    #[writable]
    pub metadata: Address,

    #[signer]
    #[writable]
    pub user: Address,

    pub system_program: Address,
    pub token_program: Address,
    pub associated_token_program: Address,
    pub rent: Address,
    pub event_authority: Address,
    pub program: Address,
}

impl CreateAccounts {
    pub fn new(mint: Address, user: Address) -> Self {
        let program = PumpInstruction::PROGRAM;

        let token_program = Address::from_str_const("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");

        let (bonding_curve, _bump) = Address::pda(&program, &[b"bonding-curve", mint.as_ref()]);
        let (associated_bonding_curve, _bump) = ata(&bonding_curve, &token_program, &mint);

        let mint_authority = Address::from_str_const("TSLvdd1pWpHVjahSpsvCXUbgwsL3JAcvokwaKt1eokM");
        let global = Address::from_str_const("4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf");
        let system_program = Address::from_str_const("11111111111111111111111111111111");

        let event_authority =
            Address::from_str_const("Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1");

        let rent = Address::from_str_const("SysvarRent111111111111111111111111111111111");
        let associated_token_program =
            Address::from_str_const("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL");

        let metaplex_program =
            Address::from_str_const("metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s");

        let (metadata, _bump) = Address::pda(
            &metaplex_program,
            &[
                b"metadata".as_ref(),
                metaplex_program.as_ref(),
                mint.as_ref(),
            ],
        );

        Self {
            mint,
            mint_authority,
            bonding_curve,
            associated_bonding_curve,
            global,
            metaplex_token_metadata_program: metaplex_program,
            metadata,
            user,
            system_program,
            token_program,
            associated_token_program,
            rent,
            event_authority,
            program,
        }
    }
}

#[derive(Accounts, Debug)]
pub struct CreateV2Accounts {
    #[signer]
    #[writable]
    pub mint: Address,
    pub mint_authority: Address,

    #[writable]
    pub bonding_curve: Address,

    #[writable]
    pub associated_bonding_curve: Address,

    pub global: Address,

    #[signer]
    #[writable]
    pub user: Address,

    pub system_program: Address,
    pub token_program: Address,
    pub associated_token_program: Address,

    #[writable]
    pub mayhem_program: Address,

    pub global_params: Address,

    #[writable]
    pub sol_vault: Address,

    #[writable]
    pub mayhem_state: Address,

    #[writable]
    pub mayhem_token_vault: Address,

    pub event_authority: Address,
    pub program: Address,
}

impl CreateV2Accounts {
    pub fn new(mint: Address, user: Address) -> Self {
        let program = PumpInstruction::PROGRAM;
        let mayhem_program = Address::from_str_const("MAyhSmzXzV1pTf7LsNkrNwkWKTo4ougAJ1PPg47MD4e");
        let token_program = Address::from_str_const("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb");
        let associated_token_program =
            Address::from_str_const("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL");
        let mint_authority = Address::from_str_const("TSLvdd1pWpHVjahSpsvCXUbgwsL3JAcvokwaKt1eokM");
        let (bonding_curve, _bump) = Address::pda(&program, &[b"bonding-curve", mint.as_ref()]);
        let (associated_bonding_curve, _bump) = ata(&bonding_curve, &token_program, &mint);

        let global = Address::from_str_const("4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf");
        let system_program = Address::from_str_const("11111111111111111111111111111111");
        let global_params = Address::from_str_const("13ec7XdrjF3h3YcqBTFDSReRcUFwbCnJaAQspM4j6DDJ");
        let sol_vault = Address::from_str_const("BwWK17cbHxwWBKZkUYvzxLcNQ1YVyaFezduWbtm2de6s");
        let (mayhem_state, _bump) =
            Address::pda(&mayhem_program, &[b"mayhem-state", mint.as_ref()]);
        let (mayhem_token_vault, _bump) = ata(&sol_vault, &token_program, &mint);

        let event_authority =
            Address::from_str_const("Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1");

        Self {
            mint,
            bonding_curve,
            user,
            mayhem_program,
            token_program,
            associated_token_program,
            mint_authority,
            associated_bonding_curve,
            global,
            system_program,
            global_params,
            sol_vault,
            mayhem_state,
            mayhem_token_vault,
            event_authority,
            program,
        }
    }
}

#[derive(Accounts, Debug)]
pub struct BuyAccounts {
    pub global: Address,
    #[writable]
    pub fee_recipient: Address,
    pub mint: Address,
    #[writable]
    pub bonding_curve: Address,
    #[writable]
    pub associated_bonding_curve: Address,
    #[writable]
    pub associated_user: Address,
    #[signer]
    #[writable]
    pub user: Address,
    pub system_program: Address,
    pub token_program: Address,
    #[writable]
    pub creator_vault: Address,
    pub event_authority: Address,
    pub program: Address,
    pub global_volume_accumulator: Address,
    #[writable]
    pub user_volume_accumulator: Address,
    pub fee_config: Address,
    pub fee_program: Address,
    pub bonding_curve_v2: Address,
    #[writable]
    pub buyback_fee_recipient: Address,
}

impl BuyAccounts {
    pub fn new(mint: Address, user: Address, creator: Address, token_program: Address) -> Self {
        let program = PumpInstruction::PROGRAM;
        let global = Address::from_str_const("4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf");
        let fee_recipient = Address::from_str_const("62qc2CNXwrYqQScmEdiZFFAnJR262PxWEuNQtxfafNgV");
        let (bonding_curve, _bump) = Address::pda(&program, &[b"bonding-curve", mint.as_ref()]);
        let (associated_bonding_curve, _bump) = ata(&bonding_curve, &token_program, &mint);
        let (associated_user, _bump) = ata(&user, &token_program, &mint);
        let system_program = Address::from_str_const("11111111111111111111111111111111");
        let (creator_vault, _bump) = Address::pda(&program, &[b"creator-vault", creator.as_ref()]);
        let event_authority =
            Address::from_str_const("Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1");
        let global_volume_accumulator =
            Address::from_str_const("Hq2wp8uJ9jCPsYgNHex8RtqdvMPfVGoYwjvF1ATiwn2Y");
        let (user_volume_accumulator, _bump) =
            Address::pda(&program, &[b"user_volume_accumulator", user.as_ref()]);
        let fee_config = Address::from_str_const("8Wf5TiAheLUqBrKXeYg2JtAFFMWtKdG2BSFgqUcPVwTt");
        let fee_program = Address::from_str_const("pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ");
        let (bonding_curve_v2, _bump) =
            Address::pda(&program, &[b"bonding-curve-v2", mint.as_ref()]);
        let buyback_fee_recipient = Address::from_str_const(PUMP_BUYBACK_FEE_RECIPIENTS[0]);
        Self {
            global,
            fee_recipient,
            mint,
            bonding_curve,
            associated_bonding_curve,
            associated_user,
            user,
            system_program,
            token_program,
            creator_vault,
            event_authority,
            program,
            global_volume_accumulator,
            user_volume_accumulator,
            fee_config,
            fee_program,
            bonding_curve_v2,
            buyback_fee_recipient,
        }
    }
}

/// Pump bonding-curve legacy `sell` accounts (Feb 2026+ layout).
///
/// Matches on-chain ordering after global buyback: `bonding_curve_v2`, then
/// `buyback_fee_recipient` (same tail as [`BuyAccounts`]).
///
/// Cashback (`bonding_curve` byte 82): insert writable `user_volume_accumulator`
/// immediately before `bonding_curve_v2` (IDL remaining-accounts layout).
///
/// Wrong account order still surfaces as `Custom(6024)` (overflow) on-chain.
#[derive(Debug)]
pub struct SellAccounts {
    pub cashback_enabled: bool,
    pub global: Address,
    pub fee_recipient: Address,
    pub mint: Address,
    pub bonding_curve: Address,
    pub associated_bonding_curve: Address,
    pub associated_user: Address,
    pub user: Address,
    pub system_program: Address,
    pub creator_vault: Address,
    pub token_program: Address,
    pub event_authority: Address,
    pub program: Address,
    pub fee_config: Address,
    pub fee_program: Address,
    pub user_volume_accumulator: Address,
    pub bonding_curve_v2: Address,
    pub buyback_fee_recipient: Address,
}

impl IntoAccountMetaArray for SellAccounts {
    fn accounts_meta(self) -> alloc::vec::Vec<AccountMeta> {
        let mut v = alloc::vec::Vec::with_capacity(16 + usize::from(self.cashback_enabled));
        v.push(AccountMeta {
            pubkey: self.global,
            is_signer: false,
            writable: false,
        });
        v.push(AccountMeta {
            pubkey: self.fee_recipient,
            is_signer: false,
            writable: true,
        });
        v.push(AccountMeta {
            pubkey: self.mint,
            is_signer: false,
            writable: false,
        });
        v.push(AccountMeta {
            pubkey: self.bonding_curve,
            is_signer: false,
            writable: true,
        });
        v.push(AccountMeta {
            pubkey: self.associated_bonding_curve,
            is_signer: false,
            writable: true,
        });
        v.push(AccountMeta {
            pubkey: self.associated_user,
            is_signer: false,
            writable: true,
        });
        v.push(AccountMeta {
            pubkey: self.user,
            is_signer: true,
            writable: true,
        });
        v.push(AccountMeta {
            pubkey: self.system_program,
            is_signer: false,
            writable: false,
        });
        v.push(AccountMeta {
            pubkey: self.creator_vault,
            is_signer: false,
            writable: true,
        });
        v.push(AccountMeta {
            pubkey: self.token_program,
            is_signer: false,
            writable: false,
        });
        v.push(AccountMeta {
            pubkey: self.event_authority,
            is_signer: false,
            writable: false,
        });
        v.push(AccountMeta {
            pubkey: self.program,
            is_signer: false,
            writable: false,
        });
        v.push(AccountMeta {
            pubkey: self.fee_config,
            is_signer: false,
            writable: false,
        });
        v.push(AccountMeta {
            pubkey: self.fee_program,
            is_signer: false,
            writable: false,
        });
        if self.cashback_enabled {
            v.push(AccountMeta {
                pubkey: self.user_volume_accumulator,
                is_signer: false,
                writable: true,
            });
        }
        v.push(AccountMeta {
            pubkey: self.bonding_curve_v2,
            is_signer: false,
            writable: false,
        });
        v.push(AccountMeta {
            pubkey: self.buyback_fee_recipient,
            is_signer: false,
            writable: true,
        });
        v
    }
}

impl SellAccounts {
    pub fn new(
        mint: Address,
        user: Address,
        creator: Address,
        token_program: Address,
        cashback_enabled: bool,
    ) -> Self {
        let program = PumpInstruction::PROGRAM;
        let global = Address::from_str_const("4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf");
        let fee_recipient = Address::from_str_const("62qc2CNXwrYqQScmEdiZFFAnJR262PxWEuNQtxfafNgV");
        let (bonding_curve, _bump) = Address::pda(&program, &[b"bonding-curve", mint.as_ref()]);
        let (associated_bonding_curve, _bump) = ata(&bonding_curve, &token_program, &mint);
        let (associated_user, _bump) = ata(&user, &token_program, &mint);
        let system_program = Address::from_str_const("11111111111111111111111111111111");
        let (creator_vault, _bump) = Address::pda(&program, &[b"creator-vault", creator.as_ref()]);
        let event_authority =
            Address::from_str_const("Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1");
        let fee_config = Address::from_str_const("8Wf5TiAheLUqBrKXeYg2JtAFFMWtKdG2BSFgqUcPVwTt");
        let fee_program = Address::from_str_const("pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ");
        let (bonding_curve_v2, _bump) =
            Address::pda(&program, &[b"bonding-curve-v2", mint.as_ref()]);
        let (user_volume_accumulator, _bump) =
            Address::pda(&program, &[b"user_volume_accumulator", user.as_ref()]);
        let buyback_fee_recipient = Address::from_str_const(PUMP_BUYBACK_FEE_RECIPIENTS[0]);
        Self {
            cashback_enabled,
            global,
            fee_recipient,
            mint,
            bonding_curve,
            associated_bonding_curve,
            associated_user,
            user,
            system_program,
            creator_vault,
            token_program,
            event_authority,
            program,
            fee_config,
            fee_program,
            user_volume_accumulator,
            bonding_curve_v2,
            buyback_fee_recipient,
        }
    }
}

#[derive(Accounts, Debug)]
pub struct CloseUserVolumeAccumulatorAccounts {
    #[signer]
    #[writable]
    user: Address,
    #[writable]
    user_volume_accumulator: Address,
    event_authority: Address,
    program: Address,
}
