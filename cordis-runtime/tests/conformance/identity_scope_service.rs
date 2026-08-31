use super::support::{Plugin, capture_context, service_key};
use cordis_core::{CordisError, InvocationKey, ServiceKey};
use cordis_runtime::Runtime;

#[test]
fn svc_001_service_key_is_logical_identity() {
    let key = ServiceKey::new("agent", "memory", 7);
    assert_eq!(key, ServiceKey::new("agent", "memory", 7));
    assert_ne!(key, ServiceKey::new("agent", "memory", 8));
    assert_eq!(key.namespace(), "agent");
    assert_eq!(key.name(), "memory");
    assert_eq!(key.version(), 7);
}

#[test]
fn inv_001_invocation_key_is_logical_identity() {
    let key = InvocationKey::new("agent", "generate", 2);
    assert_eq!(key, InvocationKey::new("agent", "generate", 2));
    assert_ne!(key, InvocationKey::new("agent", "generate", 3));
    assert_eq!(key.namespace(), "agent");
    assert_eq!(key.name(), "generate");
    assert_eq!(key.version(), 2);
}

#[tokio::test]
async fn scp_001_scopes_form_a_rooted_strict_tree() {
    let runtime = Runtime::new();
    let child = runtime
        .create_scope(runtime.root(), "child")
        .expect("child");
    let grandchild = runtime
        .create_scope(child, "grandchild")
        .expect("grandchild");
    let (_, context) = capture_context(&runtime, grandchild, "tree-observer").await;
    assert_eq!(context.scope().expect("scope"), grandchild);
    assert_eq!(context.parent().expect("parent"), Some(child));
    assert_eq!(context.root().expect("root"), runtime.root());
}

#[tokio::test]
async fn svc_002_nearest_visible_provider_wins() {
    let runtime = Runtime::new();
    let key = service_key("nearest");
    runtime
        .install(
            runtime.root(),
            Plugin::contract("root-provider", 0, vec![], vec![key.clone()], {
                let key = key.clone();
                move |context| {
                    let key = key.clone();
                    Box::pin(async move {
                        context.provide(key, 1_u32)?;
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("root provider");
    let child = runtime
        .create_scope(runtime.root(), "child")
        .expect("child");
    runtime
        .install(
            child,
            Plugin::contract("child-provider", 0, vec![], vec![key.clone()], {
                let key = key.clone();
                move |context| {
                    let key = key.clone();
                    Box::pin(async move {
                        context.provide(key, 2_u32)?;
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("child provider");
    let grandchild = runtime
        .create_scope(child, "grandchild")
        .expect("grandchild");
    let (_, context) = capture_context(&runtime, grandchild, "consumer").await;
    assert_eq!(*context.get::<u32>(&key).expect("nearest provider"), 2);
}

#[tokio::test]
async fn svc_003_same_runtime_interning_is_stable_but_cross_runtime_is_not_claimed() {
    let runtime = Runtime::new();
    let key = service_key("symbol");
    assert_eq!(
        runtime.intern_service(&key).expect("first"),
        runtime.intern_service(&key).expect("second")
    );
}

#[tokio::test]
async fn err_001_native_service_type_mismatch_is_structured() {
    let runtime = Runtime::new();
    let key = service_key("typed");
    runtime
        .install(
            runtime.root(),
            Plugin::contract("provider", 0, vec![], vec![key.clone()], {
                let key = key.clone();
                move |context| {
                    let key = key.clone();
                    Box::pin(async move {
                        context.provide(key, 5_u32)?;
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("provider");
    let (_, context) = capture_context(&runtime, runtime.root(), "typed-consumer").await;
    assert!(
        matches!(context.get::<String>(&key), Err(CordisError::TypeMismatch(found)) if found == key)
    );
}
