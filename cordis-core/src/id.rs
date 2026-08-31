//! Generation-checked public handles.

use slotmap::new_key_type;

new_key_type! {
    /// Identifies a scope without exposing arena positions.
    pub struct ScopeId;
    /// Identifies a fiber/plugin activation.
    pub struct FiberId;
    /// Identifies a logical plugin installation.
    pub struct PluginId;
    /// Identifies an event handler.
    pub struct HandlerId;
    /// Identifies an invocation handler registration.
    pub struct InvocationHandlerId;
    /// Identifies an invocation middleware registration.
    pub struct InvocationMiddlewareId;
    /// Identifies an owned effect.
    pub struct EffectId;
    /// Identifies an owned task.
    pub struct TaskId;
}
