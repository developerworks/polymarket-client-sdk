use bon::Builder;
use secrecy::ExposeSecret as _;
use serde::Serialize;
use serde_json::Value;

use super::response::CommentType;
use crate::auth::Credentials;

/// RTDS subscription request message.
#[non_exhaustive]
#[derive(Clone, Debug, Serialize, Builder)]
pub struct SubscriptionRequest {
    /// Action type ("subscribe" or "unsubscribe")
    pub action: SubscriptionAction,
    /// List of subscriptions
    pub subscriptions: Vec<Subscription>,
}

impl SubscriptionRequest {
    /// Create a subscribe request.
    #[must_use]
    pub fn subscribe(subscriptions: Vec<Subscription>) -> Self {
        Self {
            action: SubscriptionAction::Subscribe,
            subscriptions,
        }
    }

    /// Create an unsubscribe request.
    #[must_use]
    pub fn unsubscribe(subscriptions: Vec<Subscription>) -> Self {
        Self {
            action: SubscriptionAction::Unsubscribe,
            subscriptions,
        }
    }
}

/// Subscription action type.
#[non_exhaustive]
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SubscriptionAction {
    /// Subscribe to topics
    Subscribe,
    /// Unsubscribe from topics
    Unsubscribe,
}

/// Individual subscription configuration.
///
/// # Security
///
/// When serialized, this struct exposes sensitive credentials (`clob_auth`) in plaintext.
/// Ensure subscription requests are only sent over secure WebSocket connections (`wss://`)
/// and never logged or exposed in error messages.
#[non_exhaustive]
#[derive(Clone, Debug, Builder)]
pub struct Subscription {
    /// Topic name (e.g., `crypto_prices`, `comments`)
    pub topic: String,
    /// Message type filter (e.g., `update`, `comment_created`, or `*` for all)
    pub msg_type: String,
    /// Optional filters (string or JSON object)
    pub filters: Option<String>,
    /// CLOB authentication (key, secret, passphrase)
    pub clob_auth: Option<Credentials>,
}

impl Subscription {
    /// Create a subscription for Binance crypto prices.
    #[must_use]
    pub fn crypto_prices(symbols: Option<Vec<String>>) -> Self {
        // Server expects filters as a JSON array, e.g. ["btcusdt","ethusdt"]
        let filters =
            symbols.map(|s| serde_json::to_string(&s).unwrap_or_else(|_| "[]".to_owned()));
        Self {
            topic: "crypto_prices".to_owned(),
            msg_type: "update".to_owned(),
            filters,
            clob_auth: None,
        }
    }

    /// Create a subscription for Chainlink crypto prices.
    #[must_use]
    pub fn chainlink_prices(symbol: Option<String>) -> Self {
        let filters = symbol.map(|s| format!(r#"{{"symbol":"{s}"}}"#));
        Self {
            topic: "crypto_prices_chainlink".to_owned(),
            msg_type: "*".to_owned(),
            filters,
            clob_auth: None,
        }
    }

    /// Create a subscription for activity events by event slug and type.
    #[must_use]
    pub fn activity(event_slug: impl AsRef<str>, r#type: impl Into<String>) -> Self {
        Self {
            topic: "activity".to_owned(),
            msg_type: r#type.into(),
            filters: Some(json_string_filter(serde_json::json!({
                "event_slug": event_slug.as_ref()
            }))),
            clob_auth: None,
        }
    }

    /// Create a subscription for public order matches by event slug.
    #[must_use]
    pub fn orders_matched(event_slug: impl AsRef<str>) -> Self {
        Self::activity(event_slug, "orders_matched")
    }

    /// Create a subscription for public trades by event slug.
    #[must_use]
    pub fn trades(event_slug: impl AsRef<str>) -> Self {
        Self::activity(event_slug, "trades")
    }

    /// Create a subscription for equity prices by ticker symbol.
    #[must_use]
    pub fn equity_prices(symbol: impl AsRef<str>) -> Self {
        Self {
            topic: "equity_prices".to_owned(),
            msg_type: "*".to_owned(),
            filters: Some(json_string_filter(serde_json::json!({
                "symbol": symbol.as_ref()
            }))),
            clob_auth: None,
        }
    }

    /// Create a subscription for comments.
    #[must_use]
    pub fn comments(msg_type: Option<CommentType>) -> Self {
        let type_str = msg_type.map_or("*".to_owned(), |t| {
            serde_json::to_string(&t)
                .ok()
                .and_then(|s| s.trim_matches('"').to_owned().into())
                .unwrap_or_else(|| "*".to_owned())
        });
        Self {
            topic: "comments".to_owned(),
            msg_type: type_str,
            filters: None,
            clob_auth: None,
        }
    }

    /// Set CLOB authentication for this subscription.
    #[must_use]
    pub fn with_clob_auth(mut self, credentials: Credentials) -> Self {
        self.clob_auth = Some(credentials);
        self
    }

    /// Set custom filters for this subscription.
    #[must_use]
    pub fn with_filters(mut self, filters: String) -> Self {
        self.filters = Some(filters);
        self
    }
}

fn json_string_filter(value: serde_json::Value) -> String {
    serde_json::to_string(&value.to_string()).expect("RTDS JSON string filter should serialize")
}

// Custom Serialize implementation for Subscription to handle auth fields
impl Serialize for Subscription {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap as _;

        let mut map = serializer.serialize_map(None)?;

        map.serialize_entry("topic", &self.topic)?;
        map.serialize_entry("type", &self.msg_type)?;

        if let Some(filters) = &self.filters {
            // Chainlink endpoint expects filters as a JSON string (escaped),
            // while other endpoints (like Binance crypto_prices) expect raw JSON.
            // See: https://github.com/Polymarket/rs-clob-client/issues/136
            if self.topic == "crypto_prices_chainlink" {
                // Chainlink: emit filters as string, e.g. "{\"symbol\":\"btc/usd\"}"
                map.serialize_entry("filters", filters)?;
            } else if let Ok(json_value) = serde_json::from_str::<Value>(filters) {
                // Other topics: parse and emit as raw JSON, e.g. ["btcusdt","ethusdt"]
                map.serialize_entry("filters", &json_value)?;
            } else {
                // Fallback: emit as string if not valid JSON
                map.serialize_entry("filters", filters)?;
            }
        }

        // SECURITY: Credentials are intentionally revealed here for the WebSocket auth protocol.
        // This data is only sent over wss:// connections to the RTDS server.
        if let Some(creds) = &self.clob_auth {
            let auth = serde_json::json!({
                "key": creds.key.to_string(),
                "secret": creds.secret.expose_secret(),
                "passphrase": creds.passphrase.expose_secret(),
            });
            map.serialize_entry("clob_auth", &auth)?;
        }

        map.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize_subscription_request() {
        let sub =
            Subscription::crypto_prices(Some(vec!["btcusdt".to_owned(), "ethusdt".to_owned()]));
        let request = SubscriptionRequest::subscribe(vec![sub]);

        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"action\":\"subscribe\""));
        assert!(json.contains("\"topic\":\"crypto_prices\""));
        // Filters should be a JSON array, not a comma-separated string
        assert!(json.contains("\"filters\":[\"btcusdt\",\"ethusdt\"]"));
    }

    #[test]
    fn serialize_chainlink_subscription() {
        let sub = Subscription::chainlink_prices(Some("eth/usd".to_owned()));
        let request = SubscriptionRequest::subscribe(vec![sub]);

        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"topic\":\"crypto_prices_chainlink\""));
        assert!(json.contains("\"type\":\"*\""));
        // Chainlink filters should be a JSON string (escaped), not a raw JSON object
        // See: https://github.com/Polymarket/rs-clob-client/issues/136
        assert!(
            json.contains(r#""filters":"{\"symbol\":\"eth/usd\"}""#),
            "Chainlink filters should be serialized as escaped JSON string, got: {json}"
        );
    }

    #[test]
    fn serialize_comments_subscription() {
        let sub = Subscription::comments(Some(CommentType::CommentCreated));
        let request = SubscriptionRequest::subscribe(vec![sub]);

        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"topic\":\"comments\""));
        assert!(json.contains("\"type\":\"comment_created\""));
    }

    #[test]
    fn serialize_orders_matched_subscription() {
        let sub = Subscription::orders_matched("btc-updown-5m-1777580100");
        let request = SubscriptionRequest::subscribe(vec![sub]);

        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"topic\":\"activity\""));
        assert!(json.contains("\"type\":\"orders_matched\""));
        assert!(
            json.contains(r#""filters":"{\"event_slug\":\"btc-updown-5m-1777580100\"}""#),
            "orders_matched filters should serialize as escaped JSON string, got: {json}"
        );
    }

    #[test]
    fn serialize_activity_wildcard_subscription() {
        let sub = Subscription::activity("btc-updown-5m-1777580100", "*");
        let request = SubscriptionRequest::subscribe(vec![sub]);

        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"topic\":\"activity\""));
        assert!(json.contains("\"type\":\"*\""));
        assert!(
            json.contains(r#""filters":"{\"event_slug\":\"btc-updown-5m-1777580100\"}""#),
            "activity wildcard filters should serialize as escaped JSON string, got: {json}"
        );
    }

    #[test]
    fn serialize_trades_subscription() {
        let sub = Subscription::trades("btc-updown-5m-1777579200");
        let request = SubscriptionRequest::subscribe(vec![sub]);

        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"topic\":\"activity\""));
        assert!(json.contains("\"type\":\"trades\""));
        assert!(
            json.contains(r#""filters":"{\"event_slug\":\"btc-updown-5m-1777579200\"}""#),
            "trades filters should serialize as escaped JSON string, got: {json}"
        );
    }

    #[test]
    fn serialize_equity_prices_subscription() {
        let sub = Subscription::equity_prices("AAPL");
        let request = SubscriptionRequest::subscribe(vec![sub]);

        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"topic\":\"equity_prices\""));
        assert!(json.contains("\"type\":\"*\""));
        assert!(
            json.contains(r#""filters":"{\"symbol\":\"AAPL\"}""#),
            "equity_prices filters should serialize as escaped JSON string, got: {json}"
        );
    }

    #[test]
    fn serialize_chainlink_without_filters() {
        // When no symbol is provided, there should be no filters field
        let sub = Subscription::chainlink_prices(None);
        let request = SubscriptionRequest::subscribe(vec![sub]);

        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"topic\":\"crypto_prices_chainlink\""));
        assert!(!json.contains("\"filters\""));
    }

    #[test]
    fn serialize_crypto_prices_without_filters() {
        // When no symbols are provided, there should be no filters field
        let sub = Subscription::crypto_prices(None);
        let request = SubscriptionRequest::subscribe(vec![sub]);

        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"topic\":\"crypto_prices\""));
        assert!(!json.contains("\"filters\""));
    }

    #[test]
    fn serialize_mixed_subscriptions() {
        // Verify Chainlink and Binance subscriptions serialize differently in same request
        let chainlink = Subscription::chainlink_prices(Some("btc/usd".to_owned()));
        let binance =
            Subscription::crypto_prices(Some(vec!["btcusdt".to_owned(), "ethusdt".to_owned()]));
        let request = SubscriptionRequest::subscribe(vec![chainlink, binance]);

        let json = serde_json::to_string(&request).unwrap();

        // Chainlink should have escaped string filters
        assert!(
            json.contains(r#""filters":"{\"symbol\":\"btc/usd\"}""#),
            "Chainlink filters should be escaped string, got: {json}"
        );
        // Binance should have raw JSON array filters
        assert!(
            json.contains("\"filters\":[\"btcusdt\",\"ethusdt\"]"),
            "Binance filters should be raw JSON array, got: {json}"
        );
    }

    #[test]
    fn serialize_unsubscribe_request() {
        let sub = Subscription::crypto_prices(Some(vec!["btcusdt".to_owned()]));
        let request = SubscriptionRequest::unsubscribe(vec![sub]);

        let json = serde_json::to_string(&request).unwrap();
        assert!(
            json.contains("\"action\":\"unsubscribe\""),
            "Action should be 'unsubscribe', got: {json}"
        );
        assert!(json.contains("\"topic\":\"crypto_prices\""));
        assert!(json.contains("\"type\":\"update\""));
    }

    #[test]
    fn serialize_unsubscribe_without_filters() {
        let sub = Subscription::crypto_prices(None);
        let request = SubscriptionRequest::unsubscribe(vec![sub]);

        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"action\":\"unsubscribe\""));
        assert!(json.contains("\"topic\":\"crypto_prices\""));
        assert!(
            !json.contains("\"filters\""),
            "Should have no filters field"
        );
    }

    #[test]
    fn serialize_unsubscribe_chainlink() {
        let sub = Subscription::chainlink_prices(Some("btc/usd".to_owned()));
        let request = SubscriptionRequest::unsubscribe(vec![sub]);

        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"action\":\"unsubscribe\""));
        assert!(json.contains("\"topic\":\"crypto_prices_chainlink\""));
        assert!(json.contains("\"type\":\"*\""));
    }

    #[test]
    fn serialize_unsubscribe_comments() {
        let sub = Subscription::comments(Some(CommentType::CommentCreated));
        let request = SubscriptionRequest::unsubscribe(vec![sub]);

        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"action\":\"unsubscribe\""));
        assert!(json.contains("\"topic\":\"comments\""));
        assert!(json.contains("\"type\":\"comment_created\""));
    }

    #[test]
    fn serialize_unsubscribe_orders_matched() {
        let sub = Subscription::orders_matched("btc-updown-5m-1777580100");
        let request = SubscriptionRequest::unsubscribe(vec![sub]);

        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"action\":\"unsubscribe\""));
        assert!(json.contains("\"topic\":\"activity\""));
        assert!(json.contains("\"type\":\"orders_matched\""));
        assert!(
            json.contains(r#""filters":"{\"event_slug\":\"btc-updown-5m-1777580100\"}""#),
            "orders_matched unsubscribe should include filters, got: {json}"
        );
    }

    #[test]
    fn serialize_unsubscribe_trades() {
        let sub = Subscription::trades("btc-updown-5m-1777579200");
        let request = SubscriptionRequest::unsubscribe(vec![sub]);

        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"action\":\"unsubscribe\""));
        assert!(json.contains("\"topic\":\"activity\""));
        assert!(json.contains("\"type\":\"trades\""));
        assert!(
            json.contains(r#""filters":"{\"event_slug\":\"btc-updown-5m-1777579200\"}""#),
            "trades unsubscribe should include filters, got: {json}"
        );
    }

    #[test]
    fn serialize_unsubscribe_equity_prices() {
        let sub = Subscription::equity_prices("AAPL");
        let request = SubscriptionRequest::unsubscribe(vec![sub]);

        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"action\":\"unsubscribe\""));
        assert!(json.contains("\"topic\":\"equity_prices\""));
        assert!(json.contains("\"type\":\"*\""));
        assert!(
            json.contains(r#""filters":"{\"symbol\":\"AAPL\"}""#),
            "equity_prices unsubscribe should include filters, got: {json}"
        );
    }

    #[test]
    fn serialize_unsubscribe_multiple_topics() {
        let crypto = Subscription::crypto_prices(None);
        let chainlink = Subscription::chainlink_prices(None);
        let comments = Subscription::comments(None);
        let request = SubscriptionRequest::unsubscribe(vec![crypto, chainlink, comments]);

        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"action\":\"unsubscribe\""));
        assert!(json.contains("\"topic\":\"crypto_prices\""));
        assert!(json.contains("\"topic\":\"crypto_prices_chainlink\""));
        assert!(json.contains("\"topic\":\"comments\""));
    }
}
