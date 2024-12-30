use std::{
    collections::HashMap,
    marker::PhantomData,
    net::{SocketAddr, SocketAddrV4, ToSocketAddrs},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use chrono::Utc;
use clap::CommandFactory;
use config::{
    ExecutionProtocolType, FullNodeIdentityConfig, NodeBootstrapPeerConfig, NodeNetworkConfig,
};
use futures_util::{FutureExt, StreamExt};

use monad_consensus_state::ConsensusConfig;
use monad_consensus_types::metrics::Metrics;
use monad_control_panel::ipc::ControlPanelIpcReceiver;
use monad_crypto::{
    certificate_signature::{CertificateSignature, CertificateSignaturePubKey},
    hasher::{Hasher, HasherType},
};
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_block_validator::EthValidator;
use monad_eth_txpool::EthTxPool;
use monad_executor::{Executor, ExecutorMetricsChain};
use monad_executor_glue::{LogFriendlyMonadEvent, Message, MonadEvent};
use monad_ipc::IpcReceiver;
use monad_ledger::{EthHeaderParam, MonadBlockFileLedger};
use monad_raptorcast::{RaptorCast, RaptorCastConfig};
#[cfg(feature = "full-node")]
use monad_router_filter::FullNodeRouterFilter;
use monad_state::{MonadMessage, MonadStateBuilder, VerifiedMonadMessage};
use monad_statesync::StateSync;
use monad_triedb_cache::StateBackendCache;
use monad_triedb_utils::TriedbReader;
use monad_types::{
    Deserializable, DropTimer, NodeId, Round, SeqNum, Serializable, GENESIS_SEQ_NUM,
};
use monad_updaters::{
    checkpoint::FileCheckpoint, loopback::LoopbackExecutor, parent::ParentExecutor,
    timer::TokioTimer, tokio_timestamp::TokioTimestamp,
    triedb_state_root_hash::StateRootHashTriedbPoll, BoxUpdater, Updater,
};
use monad_validator::{
    validator_set::ValidatorSetFactory, weighted_round_robin::WeightedRoundRobin,
};
use monad_wal::{wal::WALoggerConfig, PersistenceLoggerBuilder};
use opentelemetry::{
    metrics::MeterProvider,
    trace::{Span, SpanBuilder, TraceContextExt},
    Context,
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::TracerProvider;
use tokio::signal::unix::{signal, SignalKind};
use tracing::{event, info, warn, Instrument, Level};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::{
    fmt::{format::FmtSpan, Layer as FmtLayer},
    layer::SubscriberExt,
    reload::Handle,
    EnvFilter, Registry,
};

mod cli;
use cli::Cli;

mod config;
use config::{SignatureCollectionType, SignatureType};

mod error;
use error::NodeSetupError;

mod state;
use state::NodeState;

fn main() {
    let mut cmd = Cli::command();

    rayon::ThreadPoolBuilder::new()
        .num_threads(8)
        .build_global()
        .unwrap();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| e.into())
        .unwrap_or_else(|e: NodeSetupError| cmd.error(e.kind(), e).exit());

    if let Err(e) = runtime.block_on(wrapped_run(cmd)) {
        tracing::error!("monad consensus node crashed: {:?}", e);
    }
}

async fn wrapped_run(mut cmd: clap::Command) -> Result<(), ()> {
    // NodeState::setup needs to be called within a tokio runtime
    let node_state = NodeState::setup(&mut cmd).unwrap_or_else(|e| cmd.error(e.kind(), e).exit());
    drop(cmd);

    // if provider is dropped, then traces stop getting sent silently...
    let maybe_provider = node_state.otel_endpoint.as_ref().map(|endpoint| {
        build_otel_tracer_provider(endpoint, node_state.node_name.clone())
            .expect("failed to build otel monad-node")
    });

    let maybe_telemetry = match (node_state.record_otel_traces, &maybe_provider) {
        (true, Some(provider)) => {
            use opentelemetry::trace::TracerProvider;
            let tracer = provider.tracer("opentelemetry");
            Some(tracing_opentelemetry::layer().with_tracer(tracer))
        }
        _ => None,
    };

    let (filter, reload_handle) =
        tracing_subscriber::reload::Layer::new(EnvFilter::from_default_env());

    let subscriber = Registry::default()
        .with(filter)
        .with(
            FmtLayer::default()
                .json()
                .with_span_events(FmtSpan::NONE)
                .with_current_span(false)
                .with_span_list(false)
                .with_writer(std::io::stdout)
                .with_ansi(false),
        )
        .with(maybe_telemetry);

    tracing::subscriber::set_global_default(subscriber).expect("failed to set logger");

    // if provider is dropped, then traces stop getting sent silently...
    let maybe_coordinator_provider = node_state.otel_endpoint.as_ref().map(|endpoint| {
        build_otel_tracer_provider(endpoint, node_state.network_name.clone())
            .expect("failed to build otel monad-coordinator")
    });

    run(maybe_coordinator_provider, node_state, reload_handle).await
}

async fn run(
    maybe_coordinator_provider: Option<TracerProvider>,
    node_state: NodeState,
    reload_handle: Handle<EnvFilter, Registry>,
) -> Result<(), ()> {
    let checkpoint_validators_first = node_state
        .forkpoint_config
        .validator_sets
        .first()
        .expect("no validator sets")
        .clone();
    let checkpoint_validators_last = node_state
        .forkpoint_config
        .validator_sets
        .last()
        .expect("no validator sets")
        .clone();

    let router: BoxUpdater<_, _> = {
        let raptor_router = build_raptorcast_router::<
            MonadMessage<SignatureType, SignatureCollectionType, ExecutionProtocolType>,
            VerifiedMonadMessage<SignatureType, SignatureCollectionType, ExecutionProtocolType>,
        >(
            node_state.node_config.network.clone(),
            node_state.router_identity,
            &node_state.node_config.bootstrap.peers,
            &node_state.node_config.fullnode.identities,
        )
        .await;

        #[cfg(feature = "full-node")]
        let raptor_router = FullNodeRouterFilter::new(raptor_router);

        <_ as Updater<_>>::boxed(raptor_router)
    };

    let val_set_update_interval = SeqNum(50_000); // TODO configurable

    let statesync_threshold: usize = node_state.node_config.statesync_threshold.into();

    _ = std::fs::remove_file(node_state.mempool_ipc_path.as_path());
    _ = std::fs::remove_file(node_state.control_panel_ipc_path.as_path());
    _ = std::fs::remove_file(node_state.statesync_ipc_path.as_path());

    // FIXME this is super jank... we should always just pass the 1 file in monad-node
    let mut statesync_triedb_path = node_state.triedb_path.clone();
    if let Ok(files) = std::fs::read_dir(&statesync_triedb_path) {
        let mut files: Vec<_> = files.collect();
        assert_eq!(files.len(), 1, "nothing in triedb path");
        statesync_triedb_path = files
            .pop()
            .unwrap()
            .expect("failed to read triedb path")
            .path();
    }
    let mut executor = ParentExecutor {
        router,
        timer: TokioTimer::default(),
        ledger: MonadBlockFileLedger::new(
            node_state.execution_ledger_path,
            node_state.bft_block_header_path,
            node_state.bft_block_payload_path,
            EthHeaderParam {
                gas_limit: node_state.node_config.consensus.block_gas_limit,
            },
        ),
        checkpoint: FileCheckpoint::new(node_state.forkpoint_path),
        state_root_hash: StateRootHashTriedbPoll::new(
            &node_state.triedb_path,
            // Use the more recent of the 2 checkpoint validator sets for seeding the default e+2
            // validator set. This allows us to manually change validator set e+1 without it
            // getting rolled back in e+2.
            // FIXME this should be deleted post staking module
            checkpoint_validators_last.validators,
            val_set_update_interval,
        ),
        timestamp: TokioTimestamp::new(Duration::from_millis(5), 100, 10001),
        ipc: IpcReceiver::new(
            node_state.mempool_ipc_path,
            node_state.node_config.ipc_tx_batch_size as usize, // tx_batch_size
            node_state.node_config.ipc_max_queued_batches as usize, // max_queued_batches
            node_state.node_config.ipc_queued_batches_watermark as usize, // queued_batches_watermark
        )
        .expect("uds bind failed"),
        control_panel: ControlPanelIpcReceiver::new(
            node_state.control_panel_ipc_path,
            reload_handle,
            1000,
        )
        .expect("uds bind failed"),
        loopback: LoopbackExecutor::default(),
        state_sync: StateSync::<SignatureType, SignatureCollectionType>::new(
            vec![statesync_triedb_path.to_string_lossy().to_string()],
            node_state.genesis_path.to_string_lossy().to_string(),
            checkpoint_validators_first
                .validators
                .0
                .iter()
                .map(|validator| validator.node_id)
                .filter(|node_id| node_id != &NodeId::new(node_state.secp256k1_identity.pubkey()))
                .collect(),
            node_state
                .node_config
                .statesync_max_concurrent_requests
                .into(),
            Duration::from_millis(node_state.node_config.statesync_request_timeout_ms.into()),
            Duration::from_millis(node_state.node_config.statesync_request_timeout_ms.into()),
            node_state
                .statesync_ipc_path
                .to_str()
                .expect("invalid file name")
                .to_owned(),
        ),
    };

    let logger_config: WALoggerConfig<LogFriendlyMonadEvent<_, _, _>> = WALoggerConfig::new(
        node_state.wal_path.clone(), // output wal path
        false,                       // flush on every write
    );
    let Ok(mut wal) = logger_config.build() else {
        event!(
            Level::ERROR,
            path = node_state.wal_path.as_path().display().to_string(),
            "failed to initialize wal",
        );
        return Err(());
    };

    let triedb_handle = TriedbReader::try_new(node_state.triedb_path.as_path())
        .expect("triedb should exist in path");
    let mut last_ledger_tip = triedb_handle
        .get_latest_finalized_block()
        .unwrap_or(SeqNum(0));
    let builder = MonadStateBuilder {
        validator_set_factory: ValidatorSetFactory::default(),
        leader_election: WeightedRoundRobin::default(),
        #[cfg(feature = "full-node")]
        transaction_pool: EthTxPool::new(false),
        #[cfg(not(feature = "full-node"))]
        transaction_pool: EthTxPool::new(true),
        block_validator: EthValidator::new(
            node_state.node_config.consensus.block_txn_limit,
            node_state.node_config.consensus.block_gas_limit,
            node_state.node_config.chain_id,
        ),
        // TODO: use PassThruBlockPolicy and NopStateBackend for consensus only
        // mode
        block_policy: EthBlockPolicy::new(
            GENESIS_SEQ_NUM, // FIXME: MonadStateBuilder is responsible for updating this to forkpoint root if necessary
            node_state.node_config.consensus.execution_delay,
            node_state.node_config.chain_id,
        ),
        state_backend: StateBackendCache::new(
            triedb_handle,
            SeqNum(node_state.node_config.consensus.execution_delay),
        ),
        key: node_state.secp256k1_identity,
        certkey: node_state.bls12_381_identity,
        val_set_update_interval,
        epoch_start_delay: Round(50),
        beneficiary: node_state.node_config.beneficiary,
        forkpoint: node_state.forkpoint_config.into(),
        consensus_config: ConsensusConfig {
            execution_delay: SeqNum(node_state.node_config.consensus.execution_delay),
            proposal_txn_limit: node_state.node_config.consensus.block_txn_limit,
            delta: Duration::from_millis(node_state.node_config.network.max_rtt_ms / 2),
            // StateSync -> Live transition happens here
            statesync_to_live_threshold: SeqNum(statesync_threshold as u64),
            // Live -> StateSync transition happens here
            live_to_statesync_threshold: SeqNum(statesync_threshold as u64 * 3 / 2),
            // Live starts execution here
            start_execution_threshold: SeqNum(statesync_threshold as u64 / 2),
            vote_pace: Duration::from_millis(1000),
            timestamp_latency_estimate_ms: 20,
        },
        _phantom: PhantomData,
    };

    let (mut state, init_commands) = builder.build();
    executor.exec(init_commands);

    let network_name_hash = {
        let mut hasher = HasherType::new();
        hasher.update(&node_state.network_name);
        let hash = hasher.hash();
        u64::from_le_bytes(
            hash.0[..std::mem::size_of::<u64>()]
                .try_into()
                .expect("u64 is 8 bytes"),
        )
    };
    let mut otel_context = maybe_coordinator_provider
        .as_ref()
        .map(|provider| build_otel_context(provider, network_name_hash));

    let mut ledger_span = tracing::info_span!("ledger_span", last_ledger_tip = last_ledger_tip.0);

    if let Some((cx, _expiry)) = &otel_context {
        ledger_span.set_parent(cx.clone());
    }

    // if provider is dropped, then traces stop getting sent silently...
    let maybe_otel_meter_provider = node_state.otel_endpoint.and_then(|otel_endpoint| {
        let record_metrics_interval = node_state.record_metrics_interval?;
        let provider = build_otel_meter_provider(
            &otel_endpoint,
            node_state.node_name.clone(),
            record_metrics_interval,
        )
        .expect("failed to build otel monad-node");
        Some(provider)
    });
    let maybe_otel_meter = maybe_otel_meter_provider
        .as_ref()
        .map(|provider| provider.meter("opentelemetry"));
    let mut gauge_cache = HashMap::new();
    let mut maybe_metrics_ticker =
        node_state
            .record_metrics_interval
            .map(|record_metrics_interval| {
                let mut timer = tokio::time::interval(record_metrics_interval);
                timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                timer
            });

    let mut sigterm = signal(SignalKind::terminate()).expect("in tokio rt");
    let mut sigint = signal(SignalKind::interrupt()).expect("in tokio rt");

    loop {
        tokio::select! {
            biased; // events are in order of priority

            result = sigterm.recv() => {
                info!(?result, "received SIGTERM, exiting...");
                break;
            }
            result = sigint.recv() => {
                info!(?result, "received SIGINT, exiting...");
                break;
            }
            _ = match &mut maybe_metrics_ticker {
                Some(ticker) => ticker.tick().boxed(),
                None => futures_util::future::pending().boxed(),
            } => {
                let otel_meter = maybe_otel_meter.as_ref().expect("otel_endpoint must have been set");
                let state_metrics = state.metrics();
                let executor_metrics = executor.metrics();
                send_metrics(otel_meter, &mut gauge_cache, state_metrics, executor_metrics);
            }
            event = executor.next().instrument(ledger_span.clone()) => {
                let Some(event) = event else {
                    event!(Level::ERROR, "parent executor returned none!");
                    return Err(());
                };
                let event_debug = {
                    let _timer = DropTimer::start(Duration::from_millis(1), |elapsed| {
                        warn!(
                            ?elapsed,
                            ?event,
                            "long time to format event"
                        )
                    });
                    format!("{:?}", event)
                };

                let event = LogFriendlyMonadEvent {
                    timestamp: Utc::now(),
                    event,
                };

                {
                    let _ledger_span = ledger_span.enter();
                    let _wal_event_span = tracing::trace_span!("wal_event_span").entered();
                    if let Err(err) = wal.push(&event) {
                        event!(Level::ERROR, ?err, "failed to push to wal",);
                        return Err(());
                    }
                };

                let commands = {
                    let _timer = DropTimer::start(Duration::from_millis(50), |elapsed| {
                        warn!(
                            ?elapsed,
                            event =? event_debug,
                            "long time to update event"
                        )
                    });
                    let _ledger_span = ledger_span.enter();
                    let _event_span = tracing::trace_span!("event_span", ?event.event).entered();
                    state.update(event.event)
                };

                if !commands.is_empty() {
                    let num_commands = commands.len();
                    let _timer = DropTimer::start(Duration::from_millis(50), |elapsed| {
                        warn!(
                            ?elapsed,
                            event =? event_debug,
                            num_commands,
                            "long time to execute commands"
                        )
                    });
                    let _ledger_span = ledger_span.enter();
                    let _exec_span = tracing::trace_span!("exec_span", num_commands).entered();
                    executor.exec(commands);
                }

                let ledger_tip = executor.ledger.last_commit().unwrap_or(last_ledger_tip);


                if ledger_tip > last_ledger_tip {
                    last_ledger_tip = ledger_tip;
                    ledger_span = tracing::info_span!("ledger_span", last_ledger_tip = last_ledger_tip.0);

                    if let Some((cx, expiry)) = &mut otel_context {
                        if *expiry < SystemTime::now() {
                            let (new_cx, new_expiry) = build_otel_context(
                                maybe_coordinator_provider
                                    .as_ref()
                                    .expect("coordinator must exist"),
                                    network_name_hash,
                            );
                            *cx = new_cx;
                            *expiry = new_expiry;
                        }
                        ledger_span.set_parent(cx.clone());
                    }
                }
            }
        }
    }

    Ok(())
}

async fn build_raptorcast_router<M, OM>(
    network_config: NodeNetworkConfig,
    identity: <SignatureType as CertificateSignature>::KeyPairType,
    peers: &[NodeBootstrapPeerConfig],
    full_nodes: &[FullNodeIdentityConfig],
) -> RaptorCast<
    SignatureType,
    M,
    OM,
    MonadEvent<SignatureType, SignatureCollectionType, ExecutionProtocolType>,
>
where
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<SignatureType>>
        + Deserializable<Bytes>
        + From<OM>
        + Send
        + Sync
        + 'static,
    <M as Deserializable<Bytes>>::ReadError: 'static,
    OM: Serializable<Bytes> + Clone + Send + Sync + 'static,
{
    RaptorCast::new(RaptorCastConfig {
        key: identity,
        known_addresses: peers
            .iter()
            .map(|peer| (build_node_id(peer), resolve_domain(&peer.address)))
            .collect(),
        full_nodes: full_nodes
            .iter()
            .map(|full_node| NodeId::new(full_node.secp256k1_pubkey))
            .collect(),
        redundancy: 3,
        local_addr: SocketAddr::V4(SocketAddrV4::new(
            network_config.bind_address_host,
            network_config.bind_address_port,
        ))
        .to_string(),
        up_bandwidth_mbps: network_config.max_mbps.into(),
    })
}

fn build_node_id(peer: &NodeBootstrapPeerConfig) -> NodeId<monad_secp::PubKey> {
    NodeId::new(peer.secp256k1_pubkey.to_owned())
}

fn resolve_domain(domain: &String) -> SocketAddr {
    domain
        .to_socket_addrs()
        .unwrap_or_else(|err| panic!("unable to resolve address={}, err={:?}", domain, err))
        .next()
        .unwrap_or_else(|| panic!("couldn't look up address={}", domain))
}

/// Returns (otel_context, expiry)
fn build_otel_context(provider: &TracerProvider, network_name_hash: u64) -> (Context, SystemTime) {
    const ROUND_SECONDS: u64 = 60; // 1 minute

    let (start_time, start_seconds) = {
        let unix_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("can't compute elapsed time");
        let start_seconds = unix_ts.as_secs() / ROUND_SECONDS * ROUND_SECONDS;
        let rounded_duration = Duration::from_secs(start_seconds);
        (UNIX_EPOCH + rounded_duration, start_seconds)
    };
    let context = {
        let span = SpanBuilder::from_name("exec")
            .with_trace_id(((network_name_hash as u128) << 64 | u128::from(start_seconds)).into())
            .with_span_id(15.into())
            .with_start_time(start_time)
            .with_end_time(start_time + Duration::from_secs(1));
        use opentelemetry::trace::{Tracer, TracerProvider};
        let span = provider
            .tracer("opentelemetry")
            .build_with_context(span, &Context::default());
        span.span_context().clone()
    };

    (
        Context::default().with_remote_span_context(context),
        start_time + Duration::from_secs(ROUND_SECONDS),
    )
}

fn send_metrics(
    meter: &opentelemetry::metrics::Meter,
    gauge_cache: &mut HashMap<&'static str, opentelemetry::metrics::Gauge<u64>>,
    state_metrics: &Metrics,
    executor_metrics: ExecutorMetricsChain,
) {
    for (k, v) in state_metrics
        .metrics()
        .into_iter()
        .chain(executor_metrics.into_inner())
    {
        let gauge = gauge_cache
            .entry(k)
            .or_insert_with(|| meter.u64_gauge(k).try_init().unwrap());
        gauge.record(v, &[]);
    }
}

fn build_otel_tracer_provider(
    otel_endpoint: &str,
    service_name: String,
) -> Result<opentelemetry_sdk::trace::TracerProvider, NodeSetupError> {
    let exporter = opentelemetry_otlp::SpanExporterBuilder::Tonic(
        opentelemetry_otlp::new_exporter()
            .tonic()
            .with_endpoint(otel_endpoint),
    )
    .build_span_exporter()?;

    let provider_builder = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_config(opentelemetry_sdk::trace::Config::default().with_resource(
            opentelemetry_sdk::Resource::new(vec![opentelemetry::KeyValue::new(
                opentelemetry_semantic_conventions::resource::SERVICE_NAME,
                service_name,
            )]),
        ))
        .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio);

    Ok(provider_builder.build())
}

fn build_otel_meter_provider(
    otel_endpoint: &str,
    service_name: String,
    interval: Duration,
) -> Result<opentelemetry_sdk::metrics::SdkMeterProvider, NodeSetupError> {
    let exporter = opentelemetry_otlp::MetricsExporterBuilder::Tonic(
        opentelemetry_otlp::new_exporter()
            .tonic()
            .with_endpoint(otel_endpoint),
    )
    .build_metrics_exporter(
        Box::<opentelemetry_sdk::metrics::reader::DefaultTemporalitySelector>::default(),
        Box::<opentelemetry_sdk::metrics::reader::DefaultAggregationSelector>::default(),
    )?;

    let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(
        exporter,
        opentelemetry_sdk::runtime::Tokio,
    )
    .with_interval(interval / 2)
    .with_timeout(interval * 2)
    .build();

    let provider_builder = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(opentelemetry_sdk::Resource::new(vec![
            opentelemetry::KeyValue::new(
                opentelemetry_semantic_conventions::resource::SERVICE_NAME,
                service_name,
            ),
        ]));

    Ok(provider_builder.build())
}
