//! ROS 2 Actions seven-state machine (`accepted`, `executing`, `blocked_approval`,
//! `canceling`, `succeeded`, `aborted`, `canceled`) and `herdr-core` signal mapping.
use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Default 270-second mobile approval hold window during which execution timeout is paused.
pub const APPROVAL_HOLD_TIMEOUT_SECS: i64 = 270;

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ActionState {
    Accepted,
    Executing,
    BlockedApproval,
    Canceling,
    Succeeded,
    Aborted,
    Canceled,
}

impl ActionState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Executing => "executing",
            Self::BlockedApproval => "blocked_approval",
            Self::Canceling => "canceling",
            Self::Succeeded => "succeeded",
            Self::Aborted => "aborted",
            Self::Canceled => "canceled",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "accepted" | "dispatch_pending" => Some(Self::Accepted),
            "executing" | "running" => Some(Self::Executing),
            "blocked_approval" | "blocked" => Some(Self::BlockedApproval),
            "canceling" => Some(Self::Canceling),
            "succeeded" => Some(Self::Succeeded),
            "aborted" | "failed" => Some(Self::Aborted),
            "canceled" | "cancelled" => Some(Self::Canceled),
            _ => None,
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Aborted | Self::Canceled)
    }

    /// Authoritative ROS 2 Action transition matrix (Spec §5.2).
    pub fn can_transition_to(self, next: Self) -> bool {
        if self == next {
            return true;
        }
        match self {
            Self::Accepted => matches!(next, Self::Executing | Self::Canceling | Self::Canceled),
            Self::Executing => matches!(
                next,
                Self::BlockedApproval | Self::Canceling | Self::Succeeded | Self::Aborted
            ),
            Self::BlockedApproval => matches!(
                next,
                Self::Executing | Self::Canceling | Self::Aborted | Self::Canceled
            ),
            Self::Canceling => matches!(next, Self::Canceled | Self::Succeeded | Self::Aborted),
            Self::Succeeded | Self::Aborted | Self::Canceled => false,
        }
    }

    pub fn transition(self, next: Self) -> Result<Self> {
        if !self.can_transition_to(next) {
            bail!(
                "illegal Action state transition: {} -> {}",
                self.as_str(),
                next.as_str()
            );
        }
        Ok(next)
    }

    /// Map an OSC 7501 status signal (`idle` | `working` | `done` | `blocked` | `error`)
    /// given the current state and whether `verify_before_done` is still pending.
    pub fn on_osc_status(self, osc_state: &str, requires_verification: bool) -> Result<Self> {
        if self.is_terminal() {
            return Ok(self);
        }
        let target = match osc_state {
            "working" => match self {
                Self::Accepted | Self::BlockedApproval | Self::Executing => Self::Executing,
                Self::Canceling => Self::Canceling,
                terminal => terminal,
            },
            "blocked" => match self {
                Self::Executing | Self::Accepted => Self::BlockedApproval,
                other => other,
            },
            "done" => {
                if requires_verification {
                    Self::Executing
                } else {
                    Self::Succeeded
                }
            }
            "error" => Self::Aborted,
            _ => self,
        };
        if self == Self::Accepted && target == Self::BlockedApproval {
            return self
                .transition(Self::Executing)?
                .transition(Self::BlockedApproval);
        }
        self.transition(target)
    }

    /// Map process exit (`Finished { exit_code }`) onto the terminal Action state.
    pub fn on_process_exit(self, exit_code: Option<i32>, requires_verification: bool) -> Self {
        if self.is_terminal() {
            return self;
        }
        if self == Self::Canceling {
            return Self::Canceled;
        }
        match exit_code {
            Some(0) if !requires_verification => Self::Succeeded,
            Some(0) => Self::Executing,
            _ => Self::Aborted,
        }
    }
}

impl fmt::Display for ActionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObservedActionView {
    pub state: ActionState,
    /// Set by supervisor when executor heartbeats are lost; reconciled via S1 upon reconnect.
    pub unconfirmed: bool,
}

pub fn is_terminal_run_state(raw: &str) -> bool {
    matches!(
        raw,
        "succeeded" | "aborted" | "canceled" | "failed" | "cancelled"
    )
}
