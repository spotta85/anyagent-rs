//! The `Runtime::login` surface: an agent's own login flow as an event stream.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::Stream;
use tokio::sync::{Notify, mpsc};

use crate::agent::AuthStatus;

/// One driven login flow. Drain `events`; call `cancel` to abort.
pub struct LoginSession {
    pub events: LoginEvents,
    pub cancel: CancelHandle,
}

/// Ordered login events; ends with exactly one `Finished`, then the stream closes.
pub struct LoginEvents(mpsc::Receiver<LoginEvent>);

impl Stream for LoginEvents {
    type Item = LoginEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_recv(cx)
    }
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub enum LoginEvent {
    /// Show or open this URL; `code` is shown next to it when present.
    OpenUrl { url: String, code: Option<String> },
    /// A raw line from the login process, for a fallback console view.
    Output { line: String },
    /// The flow is over; `status` is re-read from the agent, never assumed.
    Finished { status: AuthStatus },
}

/// Aborts the flow and kills any process it started. The stream still ends
/// with `Finished`.
#[derive(Clone)]
pub struct CancelHandle(Arc<Notify>);

impl CancelHandle {
    pub fn cancel(&self) {
        self.0.notify_one();
    }
}

/// Wires up one flow: the adapter's drive task gets the event sender and the
/// cancel signal, the caller gets the session.
pub(crate) fn login_channel() -> (mpsc::Sender<LoginEvent>, Arc<Notify>, LoginSession) {
    let (tx, rx) = mpsc::channel(16);
    let notify = Arc::new(Notify::new());
    let session = LoginSession {
        events: LoginEvents(rx),
        cancel: CancelHandle(Arc::clone(&notify)),
    };
    (tx, notify, session)
}
