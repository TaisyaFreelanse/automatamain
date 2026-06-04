use crate::{
    feed::logs::pump::{PumpEvent, TradeEvent},
    generalize::general_commands::{
        Action, Currency, GeneralBuy, GeneralCreate, GeneralMetadata, GeneralSell, TradeAction,
    },
    helper::Amount,
    launchpads::pump::general::PRECISION,
};

impl From<PumpEvent> for Action {
    fn from(val: PumpEvent) -> Self {
        match val {
            PumpEvent::Create(create_event) => Action::Create(GeneralCreate {
                mint: create_event.mint,
                user: create_event.user,
                metadata: Some(GeneralMetadata {
                    name: create_event.name,
                    ticker: create_event.symbol,
                    uri: create_event.uri,
                }),
                quote_mint: create_event.quote_mint,
            }),
            PumpEvent::TradeEvent(trade_event) => Action::Trade(trade_event.into()),
        }
    }
}

impl From<TradeEvent> for TradeAction {
    fn from(val: TradeEvent) -> Self {
        match val.is_buy {
            true => TradeAction::Buy(GeneralBuy {
                mint: val.mint,
                user: val.user,
                bought: Amount::from_raw(val.token_amount, PRECISION),
                spent: Currency::Native(Amount::from_raw_native(val.sol_amount)),
            }),
            false => TradeAction::Sell(GeneralSell {
                mint: val.mint,
                user: val.user,
                sold: Amount::from_raw(val.token_amount, PRECISION),
                received: Currency::Native(Amount::from_raw_native(val.sol_amount)),
            }),
        }
    }
}
