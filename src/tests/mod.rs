#[path = "../../tests/common/k3s.rs"]
mod k3s;

use k3s::K3s;
use dtor::dtor;
use k8s_openapi::{
    api::{
        coordination::v1::{Lease, LeaseSpec},
        core::v1::Namespace,
    },
    apimachinery::pkg::apis::meta::v1::MicroTime,
};
use kube::{
    Api, Client, Resource as _,
    api::{DeleteParams, ObjectMeta, PostParams},
};
use std::{env, sync::OnceLock, thread};
use testcontainers::{ContainerAsync, ImageExt as _, runners::AsyncRunner as _};
use tokio::{
    runtime::Runtime,
    sync::{OnceCell, RwLock},
};
use tracing::debug;

use crate::{DurationSeconds, state::LeaseStateError};

pub(crate) const TEST_NAMESPACE: &str = "kube-lease-test";

const DISABLE_CONTAINER_DESTRUCTORS_ENV_NAME: &str = "DISABLE_CONTAINER_DESTRUCTORS";

static K3S_CLUSTER_CONTAINER: OnceCell<RwLock<Option<ContainerAsync<K3s>>>> = OnceCell::const_new();
static INITIALIZED: OnceCell<bool> = OnceCell::const_new();
static TRACING: OnceLock<()> = OnceLock::new();

pub(crate) async fn init_crypto_provider() {
    static CRYPTO_PROVIDER_INITIALIZED: OnceCell<()> = OnceCell::const_new();
    CRYPTO_PROVIDER_INITIALIZED
        .get_or_init(|| async {
            if rustls::crypto::CryptoProvider::get_default().is_none() {
                rustls::crypto::ring::default_provider()
                    .install_default()
                    .expect("Error initializing rustls provider");
            }
        })
        .await;
}

pub(crate) async fn get_test_kube_client() -> anyhow::Result<Client> {
    let guard = get_k3s_cluster().await.read().await;
    let cluster = guard.as_ref().unwrap();
    K3s::get_client(cluster).await
}

async fn get_k3s_cluster() -> &'static RwLock<Option<ContainerAsync<K3s>>> {
    K3S_CLUSTER_CONTAINER
        .get_or_init(|| async {
            init_crypto_provider().await;

            let runtime_folder = std::env::temp_dir()
                .join(format!("kube-lease-k3s-{}", std::process::id()))
                .to_string_lossy()
                .into_owned();
            tokio::fs::create_dir_all(&runtime_folder).await.unwrap();

            let container = K3s::new(runtime_folder)
                .with_userns_mode("host")
                .with_privileged(true)
                .start()
                .await
                .unwrap();

            RwLock::new(Some(container))
        })
        .await
}

pub(crate) async fn init() -> Client {
    let client = get_test_kube_client().await.unwrap();
    INITIALIZED
        .get_or_init(|| async {
            create_namespace(client.clone()).await;
            init_tracing();
            true
        })
        .await;
    client
}

pub(crate) fn init_tracing() {
    TRACING.get_or_init(|| {
        tracing_subscriber::fmt::init();
    });
}

pub(crate) async fn sleep_secs(seconds: DurationSeconds) {
    tokio::time::sleep(std::time::Duration::from_secs(seconds)).await
}

pub(crate) struct LeaseDropper {
    name: String,
    ns: String,
}

impl LeaseDropper {
    pub fn new(name: impl Into<String>, ns: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ns: ns.into(),
        }
    }
}

impl Drop for LeaseDropper {
    fn drop(&mut self) {
        let name = self.name.clone();
        let ns = self.ns.clone();

        debug!(lease = %name, namespace = %ns, "dropping possibly orphaned lease");
        let _ = thread::spawn(move || {
            Runtime::new().unwrap().block_on(async {
                if let Ok(client) = get_test_kube_client().await {
                    let api = Api::<Lease>::namespaced(client, &ns);
                    let dp = DeleteParams::default();
                    let _ = api.delete(&name, &dp).await;
                }
            });
        })
        .join();
    }
}

pub(crate) async fn setup_inconsistent_lease(
    lease_name: &str,
    renew_time: Option<MicroTime>,
    acquire_time: Option<MicroTime>,
) -> Result<(), LeaseStateError> {
    let client = init().await;
    let api = Api::<Lease>::namespaced(client, TEST_NAMESPACE);

    let pp = PostParams::default();
    let spec = LeaseSpec {
        renew_time,
        acquire_time,
        ..Default::default()
    };

    let data = Lease {
        metadata: ObjectMeta {
            name: Some(lease_name.to_string()),
            ..Default::default()
        },
        spec: Some(spec),
    };

    api.create(&pp, &data).await.map_err(LeaseStateError::from)?;

    Ok(())
}

async fn create_namespace(client: Client) {
    let api = Api::<Namespace>::all(client);
    let pp = PostParams::default();

    let mut data = Namespace::default();
    data.meta_mut().name = Some(String::from(TEST_NAMESPACE));

    api.create(&pp, &data).await.unwrap_or_default();
}

#[dtor(unsafe)]
fn shutdown_test_containers() {
    static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    if env::var(DISABLE_CONTAINER_DESTRUCTORS_ENV_NAME).is_err() {
        let _ = thread::spawn(|| {
            Runtime::new().unwrap().block_on(async {
                let _guard = LOCK.lock().await;

                if let Some(k3s) = K3S_CLUSTER_CONTAINER.get() {
                    let mut k3s = k3s.write().await;
                    if k3s.is_some() {
                        let old = (*k3s).take().unwrap();
                        old.stop().await.unwrap();
                        old.rm().await.unwrap();
                        *k3s = None;
                    }
                }
            });
        })
        .join();
    }
}
