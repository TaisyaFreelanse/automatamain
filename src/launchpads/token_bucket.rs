use std::any::Any;

use solana_address::Address;

use crate::{
    general::Slot,
    generalize::general_pool::Pool,
    helper::Amount,
    trading::{
        offer::Offer,
        swarm::{SwarmActor, SwarmHandler},
        trader::TraderType,
    },
};

pub type TraderAddress = Address;

pub struct TokenBucket {
    pool: Box<dyn Pool>,
    swarm: SwarmHandler,

    created_at: Slot,
}

impl Clone for TokenBucket {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone_box(),
            swarm: self.swarm.clone(),
            created_at: self.created_at,
        }
    }
}

impl TokenBucket {
    pub fn new(pool: Box<dyn Pool>, max_supply: Amount, created_at: Slot) -> TokenBucket {
        let (mut swarm_actor, swarm_tx) =
            SwarmActor::init(max_supply, pool.token_decimals(), pool.quote_decimals());

        tokio::spawn(async move {
            swarm_actor.run().await;
        });

        Self {
            swarm: SwarmHandler::new(swarm_tx),
            pool,
            created_at,
        }
    }

    pub fn pool(&self) -> &dyn Pool {
        &*self.pool
    }

    pub fn swarm(&self) -> &SwarmHandler {
        &self.swarm
    }

    pub fn update_pool(&mut self, event: &dyn Any) {
        self.pool.update(event);
    }

    pub async fn update_swarm(
        &mut self,
        trader: TraderAddress,
        offer: Offer,
        trader_type: TraderType,
    ) {
        self.swarm.update(trader, offer, trader_type).await;
    }

    pub fn created_at(&self) -> Slot {
        self.created_at
    }
}
