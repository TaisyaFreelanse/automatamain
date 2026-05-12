use crate::helper::Amount;

pub enum Offer {
    Buy { buy_for: u64, received: Amount },

    Sell { sell_amount: Amount, received: u64 },
}
