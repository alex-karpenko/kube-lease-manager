#![allow(dead_code)]

pub mod k3s;

use dtor::dtor;
use k3s::K3s;
use std::{env, thread};
use testcontainers::{ContainerAsync, ImageExt as _, runners::AsyncRunner as _};
use tokio::{
    runtime::Runtime,
    sync::{Mutex, OnceCell, RwLock},
};

const DISABLE_CONTAINER_DESTRUCTORS_ENV_NAME: &str = "DISABLE_CONTAINER_DESTRUCTORS";

static K3S_CLUSTER_CONTAINER: OnceCell<RwLock<Option<ContainerAsync<K3s>>>> = OnceCell::const_new();

pub async fn get_test_kube_client() -> anyhow::Result<kube::Client> {
    let guard = get_k3s_cluster().await.read().await;
    let cluster = guard.as_ref().unwrap();
    K3s::get_client(cluster).await
}

pub async fn get_kubeconfig_yaml() -> anyhow::Result<String> {
    let guard = get_k3s_cluster().await.read().await;
    let cluster = guard.as_ref().unwrap();
    k3s::K3s::get_kubeconfig_yaml_with_port(cluster).await
}

async fn get_k3s_cluster() -> &'static RwLock<Option<ContainerAsync<K3s>>> {
    K3S_CLUSTER_CONTAINER
        .get_or_init(|| async {
            if rustls::crypto::CryptoProvider::get_default().is_none() {
                rustls::crypto::ring::default_provider()
                    .install_default()
                    .expect("Error initializing rustls provider");
            }

            let runtime_folder = std::env::temp_dir()
                .join(format!("kube-lease-k3s-int-{}", std::process::id()))
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

#[dtor(unsafe)]
fn shutdown_test_containers() {
    static LOCK: Mutex<()> = Mutex::const_new(());

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
