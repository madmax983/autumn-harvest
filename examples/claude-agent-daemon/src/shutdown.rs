//! One shutdown flag, shared by the drive loop and the model activity.
//!
//! `Ctrl-C` cannot be seen by the task that drives a session. The model
//! activity is synchronous, and it blocks its own thread for the whole HTTP
//! request. Nothing else on that task is polled until the request returns.
//!
//! A separate task therefore waits for the signal and raises this flag. The
//! drive loop waits on the flag, and so does the request itself, inside the
//! blocking call. Both then stop without waiting out the HTTP timeout.

use tokio::sync::watch;

/// Raises the flag once. Dropping it counts as raising it.
pub type Trigger = watch::Sender<bool>;

/// A handle on the flag. Every waiter holds its own.
#[derive(Clone)]
pub struct Signal(watch::Receiver<bool>);

/// Build the flag and the trigger that raises it.
pub fn channel() -> (Trigger, Signal) {
    let (trigger, receiver) = watch::channel(false);
    (trigger, Signal(receiver))
}

impl Signal {
    /// Wait until the daemon is asked to stop.
    ///
    /// It returns at once when the flag is ALREADY raised. A waiter that
    /// starts after the signal must not miss it, which is what a future built
    /// on the change alone would do.
    ///
    /// A dropped trigger reads as a stop. The only holder is the task that
    /// waits for the signal, so its loss means the daemon is going away.
    pub async fn raised(&mut self) {
        while !*self.0.borrow_and_update() {
            if self.0.changed().await.is_err() {
                return;
            }
        }
    }
}
