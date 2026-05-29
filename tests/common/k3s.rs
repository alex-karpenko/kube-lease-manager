#![allow(dead_code)]

use kube::{
    Config,
    config::{KubeConfigOptions, Kubeconfig},
};
use rustls::crypto::CryptoProvider;
use std::{borrow::Cow, collections::HashMap, io, path::Path};
use testcontainers::{
    ContainerAsync, Image,
    core::{ContainerPort, Mount, WaitFor},
};

pub const K3S_IMAGE_NAME: &str = "rancher/k3s";
pub const K3S_API_PORT: ContainerPort = ContainerPort::Tcp(6443);

pub const K3S_IMAGE_TAG_ENV_VAR: &str = "CARGO_TEST_K3S_IMAGE_TAG";
pub const KUBE_VERSION_ENV_VAR: &str = "CARGO_TEST_KUBE_VERSION";
pub const KUBE_VERSION_DEFAULT: &str = "1.36";

const AVAILABLE_K3S_IMAGE_TAGS: [(&str, &str); 11] = [
    ("1.36", "v1.36.1-k3s1"),
    ("1.35", "v1.35.5-k3s1"),
    ("1.34", "v1.34.8-k3s1"),
    ("1.33", "v1.33.12-k3s1"),
    ("1.32", "v1.32.13-k3s1"),
    ("1.31", "v1.31.14-k3s1"),
    ("1.30", "v1.30.13-k3s1"),
    ("1.29", "v1.29.15-k3s1"),
    ("1.28", "v1.28.15-k3s1"),
    ("1.27", "v1.27.16-k3s1"),
    ("1.26", "v1.26.15-k3s1"),
];

#[derive(Debug, Clone)]
pub struct K3s {
    kubeconfig_folder: Mount,
    tag: String,
}

impl Image for K3s {
    fn name(&self) -> &str {
        K3S_IMAGE_NAME
    }

    fn tag(&self) -> &str {
        self.tag.as_str()
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::message_on_stderr("Node controller sync successful")]
    }

    fn env_vars(&self) -> impl IntoIterator<Item = (impl Into<Cow<'_, str>>, impl Into<Cow<'_, str>>)> {
        vec![(String::from("K3S_KUBECONFIG_MODE"), String::from("644"))]
    }

    fn mounts(&self) -> impl IntoIterator<Item = &Mount> {
        vec![&self.kubeconfig_folder]
    }

    fn cmd(&self) -> impl IntoIterator<Item = impl Into<Cow<'_, str>>> {
        vec![
            "server",
            "--snapshotter=native",
            "--disable-network-policy",
            "--disable=traefik",
            "--disable=coredns",
            "--disable=servicelb",
            "--disable=local-storage",
            "--disable=metrics-server",
            "--disable-helm-controller",
        ]
    }

    fn expose_ports(&self) -> &[ContainerPort] {
        &[K3S_API_PORT]
    }
}

impl K3s {
    pub fn new(kubeconfig_folder: impl Into<String>) -> Self {
        let tag = if let Ok(tag) = std::env::var(K3S_IMAGE_TAG_ENV_VAR) {
            tag
        } else {
            let versions = AVAILABLE_K3S_IMAGE_TAGS
                .into_iter()
                .map(|(k, v)| (k, v.to_string()))
                .collect::<HashMap<&str, String>>();
            let version = std::env::var(KUBE_VERSION_ENV_VAR).unwrap_or(KUBE_VERSION_DEFAULT.to_string());
            let version = version.strip_prefix("v").unwrap_or(&version);
            let version = if version.is_empty() || version == "latest" {
                KUBE_VERSION_DEFAULT
            } else {
                version
            };

            versions
                .get(&version)
                .unwrap_or_else(|| panic!("Kube version '{version}' is not supported"))
                .to_owned()
        };

        Self {
            kubeconfig_folder: Mount::bind_mount(kubeconfig_folder, "/etc/rancher/k3s/"),
            tag,
        }
    }

    pub async fn get_kubeconfig(&self) -> io::Result<String> {
        let k3s_conf_file_path = self.kubeconfig_folder.source().unwrap();
        let k3s_conf_file_path = Path::new(k3s_conf_file_path).join("k3s.yaml");
        tokio::fs::read_to_string(k3s_conf_file_path).await
    }

    pub async fn get_client(container: &ContainerAsync<K3s>) -> anyhow::Result<kube::Client> {
        if CryptoProvider::get_default().is_none() {
            rustls::crypto::ring::default_provider()
                .install_default()
                .expect("Error initializing rustls provider");
        }

        let conf_yaml = container.image().get_kubeconfig().await?;
        let mut config = Kubeconfig::from_yaml(&conf_yaml).expect("Error loading kube config");

        let port = container.get_host_port_ipv4(K3S_API_PORT).await?;
        config.clusters.iter_mut().for_each(|cluster| {
            if let Some(server) = cluster.cluster.as_mut().and_then(|c| c.server.as_mut()) {
                *server = format!("https://127.0.0.1:{port}")
            }
        });

        let client_config = Config::from_custom_kubeconfig(config, &KubeConfigOptions::default()).await?;

        Ok(kube::Client::try_from(client_config)?)
    }

    pub async fn get_kubeconfig_yaml_with_port(container: &ContainerAsync<K3s>) -> anyhow::Result<String> {
        let conf_yaml = container.image().get_kubeconfig().await?;
        let port = container.get_host_port_ipv4(K3S_API_PORT).await?;
        Ok(conf_yaml.replace("https://127.0.0.1:6443", &format!("https://127.0.0.1:{port}")))
    }
}

pub async fn create_client_from_kubeconfig_yaml(yaml: &str) -> anyhow::Result<kube::Client> {
    if CryptoProvider::get_default().is_none() {
        rustls::crypto::ring::default_provider()
            .install_default()
            .expect("Error initializing rustls provider");
    }

    let config = Kubeconfig::from_yaml(yaml).expect("Error loading kube config");
    let client_config = Config::from_custom_kubeconfig(config, &KubeConfigOptions::default()).await?;

    Ok(kube::Client::try_from(client_config)?)
}
