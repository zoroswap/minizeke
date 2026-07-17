use base64::{Engine, engine::general_purpose};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use miden_client::{
    Deserializable, Serializable,
    account::AccountId,
    auth::{PublicKey, Signature},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    intent::Intent,
    serde::{deserialize_account_id, serialize_account_id},
};

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
    pub created_at: DateTime<Utc>,
    pub last_updated_at: DateTime<Utc>,
    pub started_processing: Option<DateTime<Utc>>,
    pub processed: Option<DateTime<Utc>>,
    pub submitted: Option<DateTime<Utc>>,
    pub confirmed: Option<DateTime<Utc>>,
    pub failed: Option<DateTime<Utc>>,
    pub executed: Option<DateTime<Utc>>,
    pub settled: Option<DateTime<Utc>>,
}

impl OrderTiming {
    pub fn new() -> Self {
        let now = Utc::now();
        Self {
            created_at: now,
            last_updated_at: now,
            started_processing: None,
            processed: None,
            submitted: None,
            confirmed: None,
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
    pub fn submitted(self) -> Self {
        let now = Utc::now();
        Self {
            last_updated_at: now,
            submitted: Some(now),
            ..self
        }
    }
    pub fn confirmed(self) -> Self {
        let now = Utc::now();
        Self {
            last_updated_at: now,
            confirmed: Some(now),
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
    Submitted(Order<Submitted>),
    Confirmed(Order<Confirmed>),
    Executed(Order<Executed>),
    Settled(Order<Settled>),
    Failed(Order<Failed>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum OrderStatus {
    Created,
    Processing,
    Processed,
    Submitted,
    Confirmed,
    Executed,
    Settled,
    Failed,
}

impl OrderStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Processing => "processing",
            Self::Processed => "processed",
            Self::Submitted => "submitted",
            Self::Confirmed => "confirmed",
            Self::Executed => "executed",
            Self::Settled => "settled",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone)]
pub struct OrderSnapshot {
    pub id: Uuid,
    pub client_order_id: Uuid,
    pub expires_at: u64,
    pub details: OrderDetails,
    pub order_type: OrderType,
    pub user_id: AccountId,
    pub timing: OrderTiming,
    pub status: OrderStatus,
    pub failure_reason: Option<OrderFailureReason>,
    pub execution_result: Option<OrderExecutionResult>,
    pub tx_hash: Option<String>,
}

impl OrderUpdate {
    pub fn snapshot(&self) -> OrderSnapshot {
        macro_rules! common {
            ($order:expr, $status:expr, $failure:expr, $result:expr, $tx_hash:expr) => {
                OrderSnapshot {
                    id: $order.id,
                    client_order_id: $order.intent.client_order_uuid(),
                    expires_at: $order.intent.expires_at,
                    details: $order.details,
                    order_type: $order.order_type,
                    user_id: $order.user_id,
                    timing: $order.timing.clone(),
                    status: $status,
                    failure_reason: $failure,
                    execution_result: $result,
                    tx_hash: $tx_hash,
                }
            };
        }

        match self {
            Self::New(order) => common!(order, OrderStatus::Created, None, None, None),
            Self::StartedProcessing(order) => {
                common!(order, OrderStatus::Processing, None, None, None)
            }
            Self::Processed(order) => common!(
                order,
                OrderStatus::Processed,
                None,
                Some(order.state.execution_result.clone()),
                None
            ),
            Self::Submitted(order) => common!(
                order,
                OrderStatus::Submitted,
                None,
                Some(order.state.execution_result.clone()),
                Some(order.state.tx_hash.clone())
            ),
            Self::Confirmed(order) => common!(
                order,
                OrderStatus::Confirmed,
                None,
                Some(order.state.execution_result.clone()),
                Some(order.state.tx_hash.clone())
            ),
            Self::Executed(order) => common!(
                order,
                OrderStatus::Executed,
                None,
                Some(order.state.execution_result.clone()),
                Some(order.state.tx_hash.clone())
            ),
            Self::Settled(order) => common!(
                order,
                OrderStatus::Settled,
                None,
                Some(order.state.execution_result.clone()),
                Some(order.state.tx_hash.clone())
            ),
            Self::Failed(order) => common!(
                order,
                OrderStatus::Failed,
                Some(order.state.reason.clone()),
                order.state.execution_result.clone(),
                order.state.tx_hash.clone()
            ),
        }
    }
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
pub struct Submitted {
    tx_hash: String,
    execution_result: OrderExecutionResult,
}

#[derive(Debug, Clone)]
pub struct Confirmed {
    tx_hash: String,
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
    tx_hash: Option<String>,
    execution_result: Option<OrderExecutionResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderExecutionResult {
    pub amount_out: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrderFailureReason {
    Expired,
    MinOutNotMet,
    InsufficientBalance,
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
impl From<Order<Submitted>> for OrderUpdate {
    fn from(value: Order<Submitted>) -> Self {
        OrderUpdate::Submitted(value)
    }
}
impl From<Order<Confirmed>> for OrderUpdate {
    fn from(value: Order<Confirmed>) -> Self {
        OrderUpdate::Confirmed(value)
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
    intent: Intent,
    details: OrderDetails,
    order_type: OrderType,
    user_id: AccountId,
    signed_order: Signature,
    pubkey: PublicKey,
    timing: OrderTiming,
}

/// Restart-safe representation of an admitted order. Runtime timing is intentionally
/// reconstructed when a worker claims the order; signed authorization data is preserved.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DurableOrder {
    pub id: Uuid,
    #[serde(serialize_with = "serialize_account_id")]
    #[serde(deserialize_with = "deserialize_account_id")]
    pub user_id: AccountId,
    pub details: OrderDetails,
    pub signed_order: String,
    pub pubkey: String,
    pub intent: Intent,
}

impl DurableOrder {
    pub fn into_created(self) -> anyhow::Result<Order<Created>> {
        let signature =
            Signature::read_from_bytes(&general_purpose::STANDARD.decode(self.signed_order)?)?;
        let pubkey = PublicKey::read_from_bytes(&general_purpose::STANDARD.decode(self.pubkey)?)?;
        Ok(Order::new_with_id(
            self.id,
            signature,
            self.user_id,
            self.details,
            pubkey,
            self.intent,
        ))
    }

    pub fn into_processed(self, amount_out: u64) -> anyhow::Result<Order<Processed>> {
        Ok(self
            .into_created()?
            .start_processing()
            .processed(OrderExecutionResult { amount_out }))
    }
}

impl Order<Created> {
    pub fn new(
        signed_order: Signature,
        user_id: AccountId,
        details: OrderDetails,
        pubkey: PublicKey,
        intent: Intent,
    ) -> Self {
        Self::new_with_id(
            Uuid::new_v4(),
            signed_order,
            user_id,
            details,
            pubkey,
            intent,
        )
    }

    pub fn new_with_id(
        id: Uuid,
        signed_order: Signature,
        user_id: AccountId,
        details: OrderDetails,
        pubkey: PublicKey,
        intent: Intent,
    ) -> Self {
        Order {
            id,
            intent,
            timing: OrderTiming::new(),
            signed_order,
            user_id,
            details,
            order_type: OrderType::Spot,
            state: Created,
            pubkey,
        }
    }

    pub fn start_processing(self) -> Order<Processing> {
        Order {
            state: Processing,
            id: self.id,
            intent: self.intent,
            order_type: self.order_type,
            details: self.details,
            user_id: self.user_id,
            signed_order: self.signed_order,
            timing: self.timing.start_processing(),
            pubkey: self.pubkey,
        }
    }
    pub fn user_id(&self) -> AccountId {
        self.user_id
    }

    pub fn client_order_id(&self) -> Uuid {
        self.intent.client_order_uuid()
    }

    pub fn durable(&self) -> DurableOrder {
        DurableOrder {
            id: self.id,
            user_id: self.user_id,
            details: self.details,
            signed_order: general_purpose::STANDARD.encode(self.signed_order.to_bytes()),
            pubkey: general_purpose::STANDARD.encode(self.pubkey.to_bytes()),
            intent: self.intent,
        }
    }
}

impl Order<Processing> {
    pub fn processed(self, execution_result: OrderExecutionResult) -> Order<Processed> {
        Order {
            state: Processed { execution_result },
            id: self.id,
            intent: self.intent,
            order_type: self.order_type,
            details: self.details,
            user_id: self.user_id,
            signed_order: self.signed_order,
            timing: self.timing.processed(),
            pubkey: self.pubkey,
        }
    }

    pub fn failed(self, reason: OrderFailureReason) -> Order<Failed> {
        Order {
            state: Failed {
                reason,
                execution_result: None,
                tx_hash: None,
            },
            id: self.id,
            intent: self.intent,
            order_type: self.order_type,
            details: self.details,
            user_id: self.user_id,
            signed_order: self.signed_order,
            timing: self.timing.failed(),
            pubkey: self.pubkey,
        }
    }

    pub fn details(&self) -> OrderDetails {
        self.details.clone()
    }
    pub fn user_id(&self) -> AccountId {
        self.user_id
    }

    pub fn expires_at(&self) -> u64 {
        self.intent.expires_at
    }
}

impl Order<Processed> {
    pub fn submitted(self, tx_hash: String) -> Order<Submitted> {
        let execution_result = self.state.execution_result;
        Order {
            state: Submitted {
                tx_hash,
                execution_result,
            },
            id: self.id,
            intent: self.intent,
            order_type: self.order_type,
            details: self.details,
            user_id: self.user_id,
            signed_order: self.signed_order,
            timing: self.timing.submitted(),
            pubkey: self.pubkey,
        }
    }
    pub fn confirmed(self, tx_hash: String) -> Order<Confirmed> {
        self.submitted(tx_hash).confirmed()
    }
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
            intent: self.intent,
            order_type: self.order_type,
            details: self.details,
            user_id: self.user_id,
            signed_order: self.signed_order,
            timing: self.timing.executed(),
            pubkey: self.pubkey,
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
                tx_hash: None,
            },
            id: self.id,
            intent: self.intent,
            order_type: self.order_type,
            details: self.details,
            user_id: self.user_id,
            signed_order: self.signed_order,
            timing: self.timing.failed(),
            pubkey: self.pubkey,
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
    pub fn signed_order(&self) -> Signature {
        self.signed_order.clone()
    }
    pub fn pubkey(&self) -> PublicKey {
        self.pubkey.clone()
    }

    pub fn intent(&self) -> Intent {
        self.intent
    }

    pub fn durable(&self) -> DurableOrder {
        DurableOrder {
            id: self.id,
            user_id: self.user_id,
            details: self.details,
            signed_order: general_purpose::STANDARD.encode(self.signed_order.to_bytes()),
            pubkey: general_purpose::STANDARD.encode(self.pubkey.to_bytes()),
            intent: self.intent,
        }
    }
}

impl Order<Submitted> {
    pub fn confirmed(self) -> Order<Confirmed> {
        Order {
            state: Confirmed {
                tx_hash: self.state.tx_hash,
                execution_result: self.state.execution_result,
            },
            id: self.id,
            intent: self.intent,
            order_type: self.order_type,
            details: self.details,
            user_id: self.user_id,
            signed_order: self.signed_order,
            timing: self.timing.confirmed(),
            pubkey: self.pubkey,
        }
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
            intent: self.intent,
            order_type: self.order_type,
            details: self.details,
            user_id: self.user_id,
            signed_order: self.signed_order,
            timing: self.timing.settled(),
            pubkey: self.pubkey,
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
    submitted: DashMap<Uuid, Order<Submitted>>,
    confirmed: DashMap<Uuid, Order<Confirmed>>,
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
            OrderUpdate::Submitted(order) => {
                self.processed.remove(&order.id);
                self.submitted.insert(order.id, order);
            }
            OrderUpdate::Confirmed(order) => {
                self.submitted.remove(&order.id);
                self.confirmed.insert(order.id, order);
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
                self.new.remove(&order.id);
                self.in_processing.remove(&order.id);
                self.processed.remove(&order.id);
                self.submitted.remove(&order.id);
                self.confirmed.remove(&order.id);
                self.executed.remove(&order.id);
                self.settled.remove(&order.id);
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
        } else if let Some(order) = self.submitted.get(id) {
            Some(order.clone().into())
        } else if let Some(order) = self.confirmed.get(id) {
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

    pub fn stats(&self) -> OrderStats {
        let by_status = OrderStatusCounts {
            created: self.new.len(),
            processing: self.in_processing.len(),
            processed: self.processed.len(),
            submitted: self.submitted.len(),
            confirmed: self.confirmed.len(),
            executed: self.executed.len(),
            settled: self.settled.len(),
            failed: self.failed.len(),
        };
        let open =
            by_status.created + by_status.processing + by_status.processed + by_status.submitted;
        let closed =
            by_status.confirmed + by_status.executed + by_status.settled + by_status.failed;
        OrderStats {
            total: open + closed,
            open,
            closed,
            by_status,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct OrderStatusCounts {
    pub created: usize,
    pub processing: usize,
    pub processed: usize,
    pub submitted: usize,
    pub confirmed: usize,
    pub executed: usize,
    pub settled: usize,
    pub failed: usize,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct OrderStats {
    pub total: usize,
    pub open: usize,
    pub closed: usize,
    pub by_status: OrderStatusCounts,
}

#[derive(Debug, Serialize)]
pub struct SerializableOrder {
    id: Uuid,
    client_order_id: Uuid,
    expires_at: u64,
    details: OrderDetails,
    order_type: OrderType,
    user_id: String,
    signed_order: String,
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
            OrderUpdate::Submitted(o) => o.into(),
            OrderUpdate::Confirmed(o) => o.into(),
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
            client_order_id: value.intent.client_order_uuid(),
            expires_at: value.intent.expires_at,
            details: value.details,
            order_type: value.order_type,
            user_id: value.user_id.to_hex(),
            signed_order: general_purpose::STANDARD.encode(value.signed_order.to_bytes()),
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
            client_order_id: value.intent.client_order_uuid(),
            expires_at: value.intent.expires_at,
            details: value.details,
            order_type: value.order_type,
            user_id: value.user_id.to_hex(),
            signed_order: general_purpose::STANDARD.encode(value.signed_order.to_bytes()),
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
            client_order_id: value.intent.client_order_uuid(),
            expires_at: value.intent.expires_at,
            details: value.details,
            order_type: value.order_type,
            user_id: value.user_id.to_hex(),
            signed_order: general_purpose::STANDARD.encode(value.signed_order.to_bytes()),
            timing: value.timing,
            failure_reason: None,
            execution_result: Some(value.state.execution_result),
            tx_hash: None,
        }
    }
}
impl From<Order<Submitted>> for SerializableOrder {
    fn from(value: Order<Submitted>) -> Self {
        Self {
            id: value.id,
            client_order_id: value.intent.client_order_uuid(),
            expires_at: value.intent.expires_at,
            details: value.details,
            order_type: value.order_type,
            user_id: value.user_id.to_hex(),
            signed_order: general_purpose::STANDARD.encode(value.signed_order.to_bytes()),
            timing: value.timing,
            failure_reason: None,
            execution_result: Some(value.state.execution_result),
            tx_hash: Some(value.state.tx_hash),
        }
    }
}
impl From<Order<Confirmed>> for SerializableOrder {
    fn from(value: Order<Confirmed>) -> Self {
        Self {
            id: value.id,
            client_order_id: value.intent.client_order_uuid(),
            expires_at: value.intent.expires_at,
            details: value.details,
            order_type: value.order_type,
            user_id: value.user_id.to_hex(),
            signed_order: general_purpose::STANDARD.encode(value.signed_order.to_bytes()),
            timing: value.timing,
            failure_reason: None,
            execution_result: Some(value.state.execution_result),
            tx_hash: Some(value.state.tx_hash),
        }
    }
}
impl From<Order<Executed>> for SerializableOrder {
    fn from(value: Order<Executed>) -> Self {
        Self {
            id: value.id,
            client_order_id: value.intent.client_order_uuid(),
            expires_at: value.intent.expires_at,
            details: value.details,
            order_type: value.order_type,
            user_id: value.user_id.to_hex(),
            signed_order: general_purpose::STANDARD.encode(value.signed_order.to_bytes()),
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
            client_order_id: value.intent.client_order_uuid(),
            expires_at: value.intent.expires_at,
            details: value.details,
            order_type: value.order_type,
            user_id: value.user_id.to_hex(),
            signed_order: general_purpose::STANDARD.encode(value.signed_order.to_bytes()),
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
            client_order_id: value.intent.client_order_uuid(),
            expires_at: value.intent.expires_at,
            details: value.details,
            order_type: value.order_type,
            user_id: value.user_id.to_hex(),
            signed_order: general_purpose::STANDARD.encode(value.signed_order.to_bytes()),
            timing: value.timing,
            failure_reason: Some(value.state.reason),
            execution_result: value.state.execution_result,
            tx_hash: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use miden_client::auth::AuthSecretKey;
    use miden_core::Word;

    use super::*;

    #[test]
    fn failed_update_evicts_order_that_is_still_new() {
        let key = AuthSecretKey::new_ecdsa_k256_keccak();
        let user_id = AccountId::from_hex("0x5a17d92af11620613414ead24f1fce").unwrap();
        let asset_in = AccountId::from_hex("0x57a179f33b726c315fcfd5e0ff3309").unwrap();
        let asset_out = AccountId::from_hex("0x1e7e8af77fc5f2f1631d5c5ce35471").unwrap();
        let intent = Intent::new_swap(
            user_id,
            asset_in,
            10,
            asset_out,
            u64::MAX,
            Uuid::nil(),
            u64::MAX,
        );
        let created = Order::new(
            key.sign(Word::default()),
            user_id,
            OrderDetails::new(asset_in, 10, asset_out, u64::MAX),
            key.public_key(),
            intent,
        );
        let failed = created
            .clone()
            .start_processing()
            .failed(OrderFailureReason::MinOutNotMet);
        let orders = Orders::default();

        orders.apply_order_update(OrderUpdate::New(created));
        orders.apply_order_update(OrderUpdate::Failed(failed));

        assert!(orders.orders_new().is_empty());
        assert_eq!(orders.orders_failed().len(), 1);
    }

    #[test]
    fn submitted_order_becomes_confirmed_only_after_confirmation_update() {
        let key = AuthSecretKey::new_ecdsa_k256_keccak();
        let user_id = AccountId::from_hex("0x5a17d92af11620613414ead24f1fce").unwrap();
        let asset_in = AccountId::from_hex("0x57a179f33b726c315fcfd5e0ff3309").unwrap();
        let asset_out = AccountId::from_hex("0x1e7e8af77fc5f2f1631d5c5ce35471").unwrap();
        let intent = Intent::new_swap(
            user_id,
            asset_in,
            10,
            asset_out,
            9,
            Uuid::new_v4(),
            u64::MAX,
        );
        let processed = Order::new(
            key.sign(Word::default()),
            user_id,
            OrderDetails::new(asset_in, 10, asset_out, 9),
            key.public_key(),
            intent,
        )
        .start_processing()
        .processed(OrderExecutionResult { amount_out: 9 });
        let order_id = processed.id;
        let submitted = processed.clone().submitted("tx-1".to_owned());
        let confirmed = submitted.clone().confirmed();
        let orders = Orders::default();

        orders.apply_order_update(OrderUpdate::Processed(processed));
        orders.apply_order_update(OrderUpdate::Submitted(submitted));
        assert_eq!(orders.stats().by_status.submitted, 1);
        assert_eq!(orders.stats().by_status.confirmed, 0);

        orders.apply_order_update(OrderUpdate::Confirmed(confirmed));
        assert_eq!(orders.stats().by_status.submitted, 0);
        assert_eq!(orders.stats().by_status.confirmed, 1);
        let snapshot = orders.get_order(&order_id).unwrap().snapshot();
        assert_eq!(snapshot.status, OrderStatus::Confirmed);
        assert_eq!(snapshot.tx_hash.as_deref(), Some("tx-1"));
        assert!(snapshot.timing.submitted.is_some());
        assert!(snapshot.timing.confirmed.is_some());
    }
}
