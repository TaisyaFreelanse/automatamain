use crate::{
    feed::logs::pump::{PumpEvent, TradeEvent},
    generalize::general_commands::{
        Action, Currency, GeneralBuy, GeneralCreate, GeneralMetadata, GeneralSell, TradeAction,
    },
    helper::Amount,
    launchpads::pump::general::PRECISION,
};

impl Into<Action> for PumpEvent {
    fn into(self) -> Action {
        match self {
            PumpEvent::Create(create_event) => Action::Create(GeneralCreate {
                mint: create_event.mint,
                user: create_event.user,
                metadata: Some(GeneralMetadata {
                    name: create_event.name,
                    ticker: create_event.symbol,
                    uri: create_event.uri,
                }),
            }),
            PumpEvent::TradeEvent(trade_event) => Action::Trade(trade_event.into()),
        }
    }
}

impl Into<TradeAction> for TradeEvent {
    fn into(self) -> TradeAction {
        match self.is_buy {
            true => TradeAction::Buy(GeneralBuy {
                mint: self.mint,
                user: self.user,
                bought: Amount::from_raw(self.token_amount, PRECISION),
                spent: Currency::Native(Amount::from_raw_native(self.sol_amount)),
            }),
            false => TradeAction::Sell(GeneralSell {
                mint: self.mint,
                user: self.user,
                sold: Amount::from_raw(self.token_amount, PRECISION),
                received: Currency::Native(Amount::from_raw_native(self.sol_amount)),
            }),
        }
    }
}
