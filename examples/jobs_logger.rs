/// This is a trivial example of `LeaseManager` usage.
///
/// It's intended to show basic lease lock management,
/// but may be used as a skeleton for a multi-pods controller or resource watcher.
///
/// `LeaseManager` is used to run ensure single instance of the logger is running at any moment of time.
/// If it currently doesn't hold the lock - it shuts down logger (if it's running).
/// If it holds the leader lock - it ensures logger is running.
/// All other time it watches on lock changes or waits for INT signal.
///
use futures::{future::ready, StreamExt};
use k8s_openapi::api::batch::v1::Job;
use kube::{
    runtime::{reflector, watcher, WatchStreamExt},
    Api, Client,
};
use kube_lease_manager::LeaseManagerBuilder;
use tokio::{
    signal::unix::{signal, SignalKind},
    sync::watch::Receiver,
    task::JoinHandle,
};
use tracing::{debug, info, trace};

const LEASE_NAME: &str = "kube-lease-manager-example-controller";

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt::init();
    let client = Client::try_default().await?;

    // Create default LeaseManager in default namespace
    let lease_manager = LeaseManagerBuilder::new(client.clone(), LEASE_NAME)
        .with_duration(5)
        .with_grace(2)
        .build()
        .await?;
    let (mut lease_channel, lease_task) = lease_manager.watch().await;

    // Create a watch channel to be able to shut down the logger task softly
    let (tx, rx) = tokio::sync::watch::channel(false);
    // Logger task handler will be here if it's running
    let mut logger_task = None;

    loop {
        let is_leader = *lease_channel.borrow_and_update();
        if is_leader && logger_task.is_none() {
            // if we hold the lock but logger isn't running - run it
            info!("got leader lock, running new job logger task");
            tx.send_replace(false); // set shutdown state to false
            logger_task = Some(watch_jobs(client.clone(), rx.clone()));
        } else if !is_leader && logger_task.is_some() {
            // if we don't hold the lock but logger is running - shutdown it
            info!("lost leader lock, shutting down job logger task");
            tx.send_replace(true); // set shutdown state to true
            tokio::join!(logger_task.unwrap()).0?; // and wait for the finish of the logger
            logger_task = None;
        }

        // wait for change of the leader or termination signal
        let mut signal_watcher = signal(SignalKind::interrupt())?;
        tokio::select! {
            _ = lease_channel.changed() => {
                debug!("lease lock state changed"); // just continue the loop
            }
            _ = signal_watcher.recv() => {
                info!("shutdown signal received");
                break;
            }
        }
    }

    // stop logger task if it's running
    if let Some(logger_task) = logger_task {
        tx.send_replace(true); // send shutdown to the logger
        tokio::join!(logger_task).0?; // and wait for the finish of the watcher
    }

    // release lease lock
    drop(lease_channel);
    let _ = tokio::join!(lease_task).0??;

    Ok(())
}

/// Spawns detached tokio task which watches on all Jobs in the cluster
/// and logs any changes on INFO level
///
/// It finishes as soon as `true` value is posted to the shutdown channel.
///
/// Returns task handler: makes it possible to wait on the finish or abort the task.
///
fn watch_jobs(client: Client, mut shutdown: Receiver<bool>) -> JoinHandle<()> {
    let jobs_api: Api<Job> = Api::all(client);
    let (_reader, writer) = reflector::store();

    // Create a watcher stream of all Jobs in the cluster
    let jobs_stream = reflector::reflector(writer, watcher(jobs_api, watcher::Config::default()))
        .applied_objects()
        .for_each(|job| {
            if let Ok(job) = job {
                trace!(?job);

                // Since we're a simple logger,
                // we just log the Jobs' name and namespace
                let name = job.metadata.name.unwrap();
                let namespace = job.metadata.namespace.unwrap();
                info!(%namespace, %name, "job updated");
            }
            ready(())
        });

    // This watcher is an async task to permanently observe two streams:
    // - possible shutdown events, in case we lost the lease lock
    // - Jobs changes events
    let watcher = async move {
        tokio::select! {
            biased;
            // Listen on shutdown events
            _ = async move {
                while shutdown.changed().await.is_ok() {
                    // In shutdown state was changed to true - break the loop,
                    // so select block finishes
                    if *shutdown.borrow_and_update() {
                        debug!("shutdown requested");
                        break;
                    }
                }
            } => {
                debug!("shutdown event loop completed");
            }
            // k8s jobs stream events
            _ = jobs_stream => {
                // Usually this branch is unreachable
                debug!("jobs stream completed");
            },
        }
    };

    // Run detached watcher task and return its handler
    tokio::spawn(watcher)
}
