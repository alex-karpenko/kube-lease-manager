/// This test creates several threads and runs `LeaseManager` on each of them.
/// Each thread creates its own `Tokio` runtime to run "workload" really concurrently
/// and tries to:
/// - lock lease using `watch()` approach;
/// - run "workload" when got the lock;
/// - release lock by dropping channel;
/// - exit.
///
/// As a result, each manager holds a lock for some time and publishes its index and lock/unlock event
/// to the channel for further analyzing.
///
/// The requirement is that a channel contains a correct sequence of events.
///
use std::{
    sync::mpsc::{self, Sender},
    thread,
    time::Duration,
};

use k8s_openapi::api::core::v1::Namespace;
use kube::{Api, Client, Resource, api::PostParams};
use kube_lease_manager::{DurationSeconds, LeaseManagerBuilder, Result};
use tracing::{debug, error};

const TEST_NAMESPACE: &str = "kube-lease-integration-test";
const TEST_LEASE_NAME: &str = "watch-many-threads";

const TEST_THREADS: usize = 5;

const TASK_DURATION: DurationSeconds = 3;
const LEASE_DURATION: DurationSeconds = 2;
const LEASE_GRACE: DurationSeconds = 1;

#[derive(Debug, PartialEq, Eq)]
enum Event {
    Locked(usize),
    Started(usize),
    Completed(usize),
    Error(usize),
}

#[test]
#[ignore = "uses k8s current-context"]
fn watch_many_threads() -> Result<()> {
    tracing_subscriber::fmt::init();

    let (tx, rx) = mpsc::channel::<Event>();
    let tasks: Vec<_> = (0..TEST_THREADS)
        .map(move |index| {
            let tx = tx.clone();
            thread::spawn(move || run_watch_tread(index, tx))
        })
        .collect();

    tasks.into_iter().for_each(|t| t.join().unwrap().unwrap());

    for _ in 0..TEST_THREADS {
        let event = rx.recv().unwrap();
        assert!(
            matches!(event, Event::Locked(_)),
            "Incorrect event type: expected `Locked`, but got {event:?}"
        );

        if let Event::Locked(index) = event {
            assert_eq!(
                rx.recv().unwrap(),
                Event::Started(index),
                "Incorrect event type or index: expected `Started({index})`, but got {event:?}"
            );
            assert_eq!(
                rx.recv().unwrap(),
                Event::Completed(index),
                "Incorrect event type or index: expected `Completed({index})`, but got {event:?}"
            );

            print!("{index} ");
        } else {
            unreachable!("Incorrect event type at previous step");
        }
    }
    println!();

    Ok(())
}

fn run_watch_tread(index: usize, tx: Sender<Event>) -> Result<()> {
    debug!(%index, "Starting");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async move {
        let client = Client::try_default().await.unwrap();
        create_namespace(client.clone(), TEST_NAMESPACE).await.unwrap();

        let manager = LeaseManagerBuilder::new(client, TEST_LEASE_NAME)
            .with_namespace(TEST_NAMESPACE)
            .with_duration(LEASE_DURATION)
            .with_grace(LEASE_GRACE)
            .build()
            .await
            .unwrap();
        let (mut channel, manager_task) = manager.watch().await;

        let mut locked = *channel.borrow_and_update();
        loop {
            if !locked {
                // Waiting for a lock
                while !locked {
                    if let Err(err) = channel.changed().await {
                        error!(%index, error = %err, "can't receive state");
                        let _ = tx.send(Event::Error(index));
                        break;
                    }
                    locked = *channel.borrow_and_update();
                    debug!(%index, "Got lock!");
                    let _ = tx.send(Event::Locked(index));
                }
            } else {
                // Wait for job completion or state change
                debug!(%index, "Try to run our job");
                let _ = tx.send(Event::Started(index));
                tokio::select! {
                    _ = job(index, TASK_DURATION) => {
                        debug!(%index, "Success!");
                        let _ = tx.send(Event::Completed(index));
                    },
                    _ = channel.changed() => {
                        error!(%index, "Unexpectedly lost the lock");
                        let _ = tx.send(Event::Error(index));
                    }
                }
                break;
            }
        }

        drop(channel);
        tokio::join!(manager_task).0.unwrap().unwrap();
    });

    Ok(())
}

async fn create_namespace(client: Client, namespace: &str) -> Result<()> {
    // Create namespace
    let pp = PostParams::default();
    let mut data = Namespace::default();
    data.meta_mut().name = Some(String::from(namespace));

    let api = Api::<Namespace>::all(client);
    let _ = api.create(&pp, &data).await;
    Ok(())
}

async fn job(index: usize, duration: DurationSeconds) {
    debug!(?index, ?duration, "Job start");
    tokio::time::sleep(Duration::from_secs(duration)).await;
    debug!(?index, ?duration, "Job finish");
}
