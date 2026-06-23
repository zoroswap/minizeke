use chrono::{DateTime, Utc};
use dashmap::DashMap;
use miden_client::account::AccountId;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::serde::{deserialize_account_id, serialize_account_id};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderType {
    Spot,
    Deposit,
    Withdraw,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub struct OrderDetails {
    #[serde(serialize_with = "serialize_account_id")]
    #[serde(deserialize_with = "deserialize_account_id")]
    pub asset_in: AccountId,
    pub amount_in: u64,
    #[serde(serialize_with = "serialize_account_id")]
    #[serde(deserialize_with = "deserialize_account_id")]
    pub asset_out: AccountId,
    pub min_amount_out: u64,
}

impl OrderDetails {
    pub fn new(
        asset_in: AccountId,
        amount_in: u64,
        asset_out: AccountId,
        min_amount_out: u64,
    ) -> Self {
        Self {
            asset_in,
            amount_in,
            asset_out,
            min_amount_out,
        }
    }
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct OrderTiming {
    created_at: DateTime<Utc>,
    last_updated_at: DateTime<Utc>,
    started_processing: Option<DateTime<Utc>>,
    processed: Option<DateTime<Utc>>,
    failed: Option<DateTime<Utc>>,
    executed: Option<DateTime<Utc>>,
    settled: Option<DateTime<Utc>>,
}

impl OrderTiming {
    pub fn new() -> Self {
        let now = Utc::now();
        Self {
            created_at: now,
            last_updated_at: now,
            started_processing: None,
            processed: None,
            failed: None,
            executed: None,
            settled: None,
        }
    }
    pub fn start_processing(self) -> Self {
        let now = Utc::now();
        Self {
            last_updated_at: now,
            started_processing: Some(now),
            ..self
        }
    }
    pub fn processed(self) -> Self {
        let now = Utc::now();
        Self {
            last_updated_at: now,
            processed: Some(now),
            ..self
        }
    }
    pub fn failed(self) -> Self {
        let now = Utc::now();
        Self {
            last_updated_at: now,
            failed: Some(now),
            ..self
        }
    }
    pub fn executed(self) -> Self {
        let now = Utc::now();
        Self {
            last_updated_at: now,
            executed: Some(now),
            ..self
        }
    }
    pub fn settled(self) -> Self {
        let now = Utc::now();
        Self {
            last_updated_at: now,
            settled: Some(now),
            ..self
        }
    }
}

#[derive(Debug, Clone)]
pub enum OrderUpdate {
    New(Order<Created>),
    StartedProcessing(Order<Processing>),
    Processed(Order<Processed>),
    Executed(Order<Executed>),
    Settled(Order<Settled>),
    Failed(Order<Failed>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum OrderStatus {
    Created,
    Processing,
    Processed,
    Executed,
    Settled,
    Failed,
}

// States of the order
#[derive(Debug, Clone)]
pub struct Created;
#[derive(Debug, Clone)]
pub struct Processing;
#[derive(Debug, Clone)]
pub struct Processed {
    execution_result: OrderExecutionResult,
}
#[derive(Debug, Clone)]
pub struct Executed {
    tx_hash: String,
    execution_result: OrderExecutionResult,
}
#[derive(Debug, Clone)]
pub struct Settled {
    tx_hash: String,
    execution_result: OrderExecutionResult,
}
#[derive(Debug, Clone)]
pub struct Failed {
    reason: OrderFailureReason,
    execution_result: Option<OrderExecutionResult>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OrderExecutionResult {
    pub amount_out: u64,
}

#[derive(Debug, Clone, Serialize)]
pub enum OrderFailureReason {
    Expired,
    MinOutNotMet,
    ExecutionError,
}

impl From<Order<Created>> for OrderUpdate {
    fn from(value: Order<Created>) -> Self {
        OrderUpdate::New(value)
    }
}
impl From<Order<Processing>> for OrderUpdate {
    fn from(value: Order<Processing>) -> Self {
        OrderUpdate::StartedProcessing(value)
    }
}
impl From<Order<Processed>> for OrderUpdate {
    fn from(value: Order<Processed>) -> Self {
        OrderUpdate::Processed(value)
    }
}
impl From<Order<Executed>> for OrderUpdate {
    fn from(value: Order<Executed>) -> Self {
        OrderUpdate::Executed(value)
    }
}
impl From<Order<Settled>> for OrderUpdate {
    fn from(value: Order<Settled>) -> Self {
        OrderUpdate::Settled(value)
    }
}
impl From<Order<Failed>> for OrderUpdate {
    fn from(value: Order<Failed>) -> Self {
        OrderUpdate::Failed(value)
    }
}

#[derive(Debug, Clone)]
pub struct Order<State> {
    state: State,
    pub id: Uuid,
    details: OrderDetails,
    order_type: OrderType,
    user_id: AccountId,
    pubkey: String,
    timing: OrderTiming,
}

impl Order<Created> {
    pub fn new(pubkey: String, user_id: AccountId, details: OrderDetails) -> Self {
        Order {
            id: Uuid::new_v4(),
            timing: OrderTiming::new(),
            pubkey,
            user_id,
            details,
            order_type: OrderType::Spot,
            state: Created,
        }
    }

    pub fn start_processing(self) -> Order<Processing> {
        Order {
            state: Processing,
            id: self.id,
            order_type: self.order_type,
            details: self.details,
            user_id: self.user_id,
            pubkey: self.pubkey,
            timing: self.timing.start_processing(),
        }
    }
    pub fn user_id(&self) -> AccountId {
        self.user_id
    }
}

impl Order<Processing> {
    pub fn processed(self, execution_result: OrderExecutionResult) -> Order<Processed> {
        Order {
            state: Processed { execution_result },
            id: self.id,
            order_type: self.order_type,
            details: self.details,
            user_id: self.user_id,
            pubkey: self.pubkey,
            timing: self.timing.processed(),
        }
    }

    pub fn details(&self) -> OrderDetails {
        self.details.clone()
    }
    pub fn user_id(&self) -> AccountId {
        self.user_id
    }
}

impl Order<Processed> {
    pub fn executed(
        self,
        tx_hash: String,
        execution_result: OrderExecutionResult,
    ) -> Order<Executed> {
        Order {
            state: Executed {
                tx_hash,
                execution_result,
            },
            id: self.id,
            order_type: self.order_type,
            details: self.details,
            user_id: self.user_id,
            pubkey: self.pubkey,
            timing: self.timing.executed(),
        }
    }
    pub fn failed(
        self,
        reason: OrderFailureReason,
        execution_result: Option<OrderExecutionResult>,
    ) -> Order<Failed> {
        Order {
            state: Failed {
                reason,
                execution_result,
            },
            id: self.id,
            order_type: self.order_type,
            details: self.details,
            user_id: self.user_id,
            pubkey: self.pubkey,
            timing: self.timing.failed(),
        }
    }
    pub fn details(&self) -> OrderDetails {
        self.details.clone()
    }
    pub fn user_id(&self) -> AccountId {
        self.user_id
    }
    pub fn execution_result(&self) -> OrderExecutionResult {
        self.state.execution_result.clone()
    }
}

impl Order<Executed> {
    pub fn settled(self) -> Order<Settled> {
        Order {
            state: Settled {
                tx_hash: self.state.tx_hash,
                execution_result: self.state.execution_result,
            },
            id: self.id,
            order_type: self.order_type,
            details: self.details,
            user_id: self.user_id,
            pubkey: self.pubkey,
            timing: self.timing.settled(),
        }
    }
    pub fn details(&self) -> OrderDetails {
        self.details.clone()
    }
    pub fn user_id(&self) -> AccountId {
        self.user_id
    }
    pub fn execution_result(&self) -> OrderExecutionResult {
        self.state.execution_result.clone()
    }
}

#[derive(Default)]
pub struct Orders {
    new: DashMap<Uuid, Order<Created>>,
    in_processing: DashMap<Uuid, Order<Processing>>,
    processed: DashMap<Uuid, Order<Processed>>,
    executed: DashMap<Uuid, Order<Executed>>,
    settled: DashMap<Uuid, Order<Settled>>,
    failed: DashMap<Uuid, Order<Failed>>,
}

impl Orders {
    pub fn apply_order_update(&self, order_update: OrderUpdate) {
        match order_update {
            OrderUpdate::New(order) => {
                self.new.insert(order.id, order);
            }
            OrderUpdate::StartedProcessing(order) => {
                self.new.remove(&order.id);
                self.in_processing.insert(order.id, order);
            }
            OrderUpdate::Processed(order) => {
                self.in_processing.remove(&order.id);
                self.processed.insert(order.id, order);
            }
            OrderUpdate::Executed(order) => {
                self.processed.remove(&order.id);
                self.executed.insert(order.id, order);
            }
            OrderUpdate::Settled(order) => {
                self.executed.remove(&order.id);
                self.settled.insert(order.id, order);
            }
            OrderUpdate::Failed(order) => {
                self.in_processing.remove(&order.id);
                self.processed.remove(&order.id);
                self.failed.insert(order.id, order);
            }
        }
    }

    pub fn get_order(&self, id: &Uuid) -> Option<OrderUpdate> {
        if let Some(order) = self.new.get(id) {
            Some(order.clone().into())
        } else if let Some(order) = self.in_processing.get(id) {
            Some(order.clone().into())
        } else if let Some(order) = self.processed.get(id) {
            Some(order.clone().into())
        } else if let Some(order) = self.executed.get(id) {
            Some(order.clone().into())
        } else if let Some(order) = self.settled.get(id) {
            Some(order.clone().into())
        } else if let Some(order) = self.failed.get(id) {
            Some(order.clone().into())
        } else {
            None
        }
    }

    pub fn orders_new(&self) -> Vec<Order<Created>> {
        self.new
            .clone()
            .into_iter()
            .map(|(_, v)| v)
            .collect::<Vec<Order<Created>>>()
    }
    pub fn orders_processing(&self) -> Vec<Order<Processing>> {
        self.in_processing
            .clone()
            .into_iter()
            .map(|(_, v)| v)
            .collect::<Vec<Order<Processing>>>()
    }
    pub fn orders_processed(&self) -> Vec<Order<Processed>> {
        self.processed
            .clone()
            .into_iter()
            .map(|(_, v)| v)
            .collect::<Vec<Order<Processed>>>()
    }
    pub fn order_executed(&self) -> Vec<Order<Executed>> {
        self.executed
            .clone()
            .into_iter()
            .map(|(_, v)| v)
            .collect::<Vec<Order<Executed>>>()
    }
    pub fn orders_settled(&self) -> Vec<Order<Settled>> {
        self.settled
            .clone()
            .into_iter()
            .map(|(_, v)| v)
            .collect::<Vec<Order<Settled>>>()
    }
    pub fn orders_failed(&self) -> Vec<Order<Failed>> {
        self.failed
            .clone()
            .into_iter()
            .map(|(_, v)| v)
            .collect::<Vec<Order<Failed>>>()
    }
}

#[derive(Debug, Serialize)]
pub struct SerializableOrder {
    id: Uuid,
    details: OrderDetails,
    order_type: OrderType,
    user_id: String,
    pubkey: String,
    timing: OrderTiming,
    failure_reason: Option<OrderFailureReason>,
    execution_result: Option<OrderExecutionResult>,
    tx_hash: Option<String>,
}

impl From<OrderUpdate> for SerializableOrder {
    fn from(value: OrderUpdate) -> Self {
        match value {
            OrderUpdate::New(o) => o.into(),
            OrderUpdate::StartedProcessing(o) => o.into(),
            OrderUpdate::Processed(o) => o.into(),
            OrderUpdate::Executed(o) => o.into(),
            OrderUpdate::Failed(o) => o.into(),
            OrderUpdate::Settled(o) => o.into(),
        }
    }
}

impl From<Order<Created>> for SerializableOrder {
    fn from(value: Order<Created>) -> Self {
        Self {
            id: value.id,
            details: value.details,
            order_type: value.order_type,
            user_id: value.user_id.to_hex(),
            pubkey: value.pubkey,
            timing: value.timing,
            failure_reason: None,
            execution_result: None,
            tx_hash: None,
        }
    }
}
impl From<Order<Processing>> for SerializableOrder {
    fn from(value: Order<Processing>) -> Self {
        Self {
            id: value.id,
            details: value.details,
            order_type: value.order_type,
            user_id: value.user_id.to_hex(),
            pubkey: value.pubkey,
            timing: value.timing,
            failure_reason: None,
            execution_result: None,
            tx_hash: None,
        }
    }
}
impl From<Order<Processed>> for SerializableOrder {
    fn from(value: Order<Processed>) -> Self {
        Self {
            id: value.id,
            details: value.details,
            order_type: value.order_type,
            user_id: value.user_id.to_hex(),
            pubkey: value.pubkey,
            timing: value.timing,
            failure_reason: None,
            execution_result: Some(value.state.execution_result),
            tx_hash: None,
        }
    }
}
impl From<Order<Executed>> for SerializableOrder {
    fn from(value: Order<Executed>) -> Self {
        Self {
            id: value.id,
            details: value.details,
            order_type: value.order_type,
            user_id: value.user_id.to_hex(),
            pubkey: value.pubkey,
            timing: value.timing,
            failure_reason: None,
            execution_result: Some(value.state.execution_result),
            tx_hash: Some(value.state.tx_hash),
        }
    }
}
impl From<Order<Settled>> for SerializableOrder {
    fn from(value: Order<Settled>) -> Self {
        Self {
            id: value.id,
            details: value.details,
            order_type: value.order_type,
            user_id: value.user_id.to_hex(),
            pubkey: value.pubkey,
            timing: value.timing,
            failure_reason: None,
            execution_result: Some(value.state.execution_result),
            tx_hash: Some(value.state.tx_hash),
        }
    }
}
impl From<Order<Failed>> for SerializableOrder {
    fn from(value: Order<Failed>) -> Self {
        Self {
            id: value.id,
            details: value.details,
            order_type: value.order_type,
            user_id: value.user_id.to_hex(),
            pubkey: value.pubkey,
            timing: value.timing,
            failure_reason: Some(value.state.reason),
            execution_result: value.state.execution_result,
            tx_hash: None,
        }
    }
}
