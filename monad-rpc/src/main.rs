use std::{path::PathBuf, sync::Arc};

use actix::prelude::*;
use actix_http::body::BoxBody;
use actix_web::{
    dev::{ServiceFactory, ServiceRequest, ServiceResponse},
    web, App, Error, HttpResponse, HttpServer,
};
use clap::Parser;
use eth_json_types::serialize_result;
use futures::{SinkExt, StreamExt};
use monad_triedb_utils::triedb_env::TriedbEnv;
use opentelemetry::metrics::MeterProvider;
use reth_primitives::TransactionSigned;
use serde_json::Value;
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};
use tracing_subscriber::{
    fmt::{format::FmtSpan, Layer as FmtLayer},
    layer::SubscriberExt,
    EnvFilter, Registry,
};

use crate::{
    account_handlers::{
        monad_eth_getBalance, monad_eth_getCode, monad_eth_getProof, monad_eth_getStorageAt,
        monad_eth_getTransactionCount, monad_eth_syncing,
    },
    block_handlers::{
        monad_eth_blockNumber, monad_eth_chainId, monad_eth_getBlockByHash,
        monad_eth_getBlockByNumber, monad_eth_getBlockReceipts,
        monad_eth_getBlockTransactionCountByHash, monad_eth_getBlockTransactionCountByNumber,
    },
    call::monad_eth_call,
    cli::Cli,
    debug::{
        monad_debug_getRawBlock, monad_debug_getRawHeader, monad_debug_getRawReceipts,
        monad_debug_getRawTransaction, monad_debug_traceCall,
    },
    eth_txn_handlers::{
        monad_eth_getLogs, monad_eth_getTransactionByBlockHashAndIndex,
        monad_eth_getTransactionByBlockNumberAndIndex, monad_eth_getTransactionByHash,
        monad_eth_getTransactionReceipt, monad_eth_sendRawTransaction,
    },
    gas_handlers::{
        monad_eth_estimateGas, monad_eth_feeHistory, monad_eth_gasPrice,
        monad_eth_maxPriorityFeePerGas,
    },
    jsonrpc::{JsonRpcError, JsonRpcResultExt, Request, RequestWrapper, Response, ResponseWrapper},
    mempool_tx::MempoolTxIpcSender,
    trace::{
        monad_trace_block, monad_trace_call, monad_trace_callMany, monad_trace_get,
        monad_trace_transaction,
    },
    trace_handlers::{
        monad_debug_traceBlockByHash, monad_debug_traceBlockByNumber, monad_debug_traceTransaction,
    },
    vpool::{
        monad_txpool_content, monad_txpool_contentFrom, monad_txpool_inspect, monad_txpool_status,
    },
    websocket::Disconnect,
};

mod account_handlers;
mod block_handlers;
mod block_watcher;
mod call;
mod cli;
mod debug;
pub mod docs;
mod eth_json_types;
mod eth_txn_handlers;
mod gas_handlers;
mod gas_oracle;
mod hex;
mod jsonrpc;
mod mempool_tx;
mod metrics;
mod trace;
mod trace_handlers;
mod vpool;
mod websocket;

async fn rpc_handler(body: bytes::Bytes, app_state: web::Data<MonadRpcResources>) -> HttpResponse {
    let request: RequestWrapper<Value> = match serde_json::from_slice(&body) {
        Ok(req) => req,
        Err(e) => {
            debug!("parse error: {e} {body:?}");
            return HttpResponse::Ok().json(Response::from_error(JsonRpcError::parse_error()));
        }
    };

    let response = match request {
        RequestWrapper::Single(json_request) => {
            let Ok(request) = serde_json::from_value::<Request>(json_request) else {
                return HttpResponse::Ok().json(Response::from_error(JsonRpcError::parse_error()));
            };
            ResponseWrapper::Single(Response::from_result(
                request.id,
                rpc_select(&app_state, &request.method, request.params).await,
            ))
        }
        RequestWrapper::Batch(json_batch_request) => {
            if json_batch_request.is_empty()
                || json_batch_request.len() > app_state.batch_request_limit as usize
            {
                return HttpResponse::Ok()
                    .json(Response::from_error(JsonRpcError::invalid_request()));
            }
            let batch_response =
                futures::future::join_all(json_batch_request.into_iter().map(|json_request| {
                    let app_state = app_state.clone(); // cheap copy
                    async move {
                        let Ok(request) = serde_json::from_value::<Request>(json_request) else {
                            return (Value::Null, Err(JsonRpcError::invalid_request()));
                        };
                        let (state, id, method, params) =
                            (app_state, request.id, request.method, request.params);
                        (id, rpc_select(&state, &method, params).await)
                    }
                }))
                .await
                .into_iter()
                .map(|(request_id, response)| Response::from_result(request_id, response))
                .collect::<Vec<_>>();
            ResponseWrapper::Batch(batch_response)
        }
    };

    // check if the response size exceeds the limit
    // return invalid request error if it does
    match serde_json::to_vec(&response) {
        Ok(bytes) => {
            if bytes.len() > app_state.max_response_size as usize {
                debug!("response exceed size limit: {body:?} => {response:?}");
                return HttpResponse::Ok()
                    .json(Response::from_error(JsonRpcError::invalid_request()));
            }
        }
        Err(e) => {
            debug!("response serialization error: {e}");
            return HttpResponse::Ok().json(Response::from_error(JsonRpcError::internal_error(
                format!("serialization error: {}", e),
            )));
        }
    };

    // log the request and response based on the response content
    match &response {
        ResponseWrapper::Single(resp) => match resp.error {
            Some(_) => info!(?body, ?response, "rpc_request/response error"),
            None => debug!(?body, ?response, "rpc_request/response successful"),
        },
        _ => debug!(?body, ?response, "rpc_batch_request/response"),
    }

    HttpResponse::Ok().json(&response)
}

async fn rpc_select(
    app_state: &MonadRpcResources,
    method: &str,
    params: Value,
) -> Result<Value, JsonRpcError> {
    match method {
        "debug_getRawBlock" => {
            let triedb_env = app_state.triedb_reader.as_ref().method_not_supported()?;
            let params = serde_json::from_value(params).invalid_params()?;
            monad_debug_getRawBlock(triedb_env, params)
                .await
                .map(serialize_result)?
        }
        "debug_getRawHeader" => {
            let triedb_env = app_state.triedb_reader.as_ref().method_not_supported()?;
            let params = serde_json::from_value(params).invalid_params()?;
            monad_debug_getRawHeader(triedb_env, params)
                .await
                .map(serialize_result)?
        }
        "debug_getRawReceipts" => {
            let triedb_env = app_state.triedb_reader.as_ref().method_not_supported()?;
            let params = serde_json::from_value(params).invalid_params()?;
            monad_debug_getRawReceipts(triedb_env, params)
                .await
                .map(serialize_result)?
        }
        "debug_getRawTransaction" => {
            let triedb_env = app_state.triedb_reader.as_ref().method_not_supported()?;
            let params = serde_json::from_value(params).invalid_params()?;
            monad_debug_getRawTransaction(triedb_env, params)
                .await
                .map(serialize_result)?
        }
        "debug_traceBlockByHash" => {
            let triedb_env = app_state.triedb_reader.as_ref().method_not_supported()?;
            let params = serde_json::from_value(params).invalid_params()?;
            monad_debug_traceBlockByHash(triedb_env, params)
                .await
                .map(serialize_result)?
        }
        "debug_traceBlockByNumber" => {
            let triedb_env = app_state.triedb_reader.as_ref().method_not_supported()?;
            let params = serde_json::from_value(params).invalid_params()?;
            monad_debug_traceBlockByNumber(triedb_env, params)
                .await
                .map(serialize_result)?
        }
        "debug_traceCall" => {
            let triedb_env = app_state.triedb_reader.as_ref().method_not_supported()?;
            let params = serde_json::from_value(params).invalid_params()?;
            monad_debug_traceCall(triedb_env, params)
                .await
                .map(serialize_result)?
        }
        "debug_traceTransaction" => {
            let triedb_env = app_state.triedb_reader.as_ref().method_not_supported()?;
            let params = serde_json::from_value(params).invalid_params()?;
            monad_debug_traceTransaction(triedb_env, params)
                .await
                .map(serialize_result)?
        }
        "eth_call" => {
            let triedb_env = app_state.triedb_reader.as_ref().method_not_supported()?;

            let Some(execution_ledger_path) = &app_state.execution_ledger_path.0 else {
                debug!("execution ledger path was not set");
                return Err(JsonRpcError::method_not_supported());
            };

            // acquire the concurrent requests permit
            let _permit = &app_state.rate_limiter.try_acquire().map_err(|_| {
                JsonRpcError::internal_error("eth_call concurrent requests limit".into())
            })?;

            let params = serde_json::from_value(params).invalid_params()?;
            monad_eth_call(
                triedb_env,
                execution_ledger_path.as_path(),
                app_state.chain_id,
                params,
            )
            .await
            .map(serialize_result)?
        }
        "eth_sendRawTransaction" => {
            let params = serde_json::from_value(params).invalid_params()?;
            monad_eth_sendRawTransaction(
                &app_state.tx_pool,
                params,
                app_state.chain_id,
                app_state.allow_unprotected_txs,
            )
            .await
            .map(serialize_result)?
        }
        "eth_getLogs" => {
            let triedb_env = app_state.triedb_reader.as_ref().method_not_supported()?;

            let params = serde_json::from_value(params).invalid_params()?;
            monad_eth_getLogs(triedb_env, params)
                .await
                .map(serialize_result)?
        }
        "eth_getTransactionByHash" => {
            if let Some(triedb_env) = app_state.triedb_reader.as_ref() {
                let params = serde_json::from_value(params).invalid_params()?;
                monad_eth_getTransactionByHash(triedb_env, params)
                    .await
                    .map(serialize_result)?
            } else {
                Err(JsonRpcError::method_not_supported())
            }
        }
        "eth_getBlockByHash" => {
            if let Some(triedb_env) = app_state.triedb_reader.as_ref() {
                let params = serde_json::from_value(params).invalid_params()?;
                monad_eth_getBlockByHash(triedb_env, params)
                    .await
                    .map(serialize_result)?
            } else {
                Err(JsonRpcError::method_not_supported())
            }
        }
        "eth_getBlockByNumber" => {
            if let Some(reader) = &app_state.triedb_reader {
                let params = serde_json::from_value(params).invalid_params()?;
                monad_eth_getBlockByNumber(reader, params)
                    .await
                    .map(serialize_result)?
            } else {
                Err(JsonRpcError::method_not_supported())
            }
        }
        "eth_getTransactionByBlockHashAndIndex" => {
            if let Some(triedb_env) = &app_state.triedb_reader {
                let params = serde_json::from_value(params).invalid_params()?;
                monad_eth_getTransactionByBlockHashAndIndex(triedb_env, params)
                    .await
                    .map(serialize_result)?
            } else {
                Err(JsonRpcError::method_not_supported())
            }
        }
        "eth_getTransactionByBlockNumberAndIndex" => {
            if let Some(triedb_env) = &app_state.triedb_reader {
                let params = serde_json::from_value(params).invalid_params()?;
                monad_eth_getTransactionByBlockNumberAndIndex(triedb_env, params)
                    .await
                    .map(serialize_result)?
            } else {
                Err(JsonRpcError::method_not_supported())
            }
        }
        "eth_getBlockTransactionCountByHash" => {
            if let Some(triedb_env) = app_state.triedb_reader.as_ref() {
                let params = serde_json::from_value(params).invalid_params()?;
                monad_eth_getBlockTransactionCountByHash(triedb_env, params)
                    .await
                    .map(serialize_result)?
            } else {
                Err(JsonRpcError::method_not_supported())
            }
        }
        "eth_getBlockTransactionCountByNumber" => {
            if let Some(triedb_env) = app_state.triedb_reader.as_ref() {
                let params = serde_json::from_value(params).invalid_params()?;
                monad_eth_getBlockTransactionCountByNumber(triedb_env, params)
                    .await
                    .map(serialize_result)?
            } else {
                Err(JsonRpcError::method_not_supported())
            }
        }
        "eth_getBalance" => {
            if let Some(reader) = &app_state.triedb_reader {
                let params = serde_json::from_value(params).invalid_params()?;
                monad_eth_getBalance(reader, params)
                    .await
                    .map(serialize_result)?
            } else {
                Err(JsonRpcError::method_not_supported())
            }
        }
        "eth_getCode" => {
            if let Some(reader) = &app_state.triedb_reader {
                let params = serde_json::from_value(params).invalid_params()?;
                monad_eth_getCode(reader, params)
                    .await
                    .map(serialize_result)?
            } else {
                Err(JsonRpcError::method_not_supported())
            }
        }
        "eth_getStorageAt" => {
            if let Some(reader) = &app_state.triedb_reader {
                let params = serde_json::from_value(params).invalid_params()?;
                monad_eth_getStorageAt(reader, params)
                    .await
                    .map(serialize_result)?
            } else {
                Err(JsonRpcError::method_not_supported())
            }
        }
        "eth_getTransactionCount" => {
            if let Some(reader) = &app_state.triedb_reader {
                let params = serde_json::from_value(params).invalid_params()?;
                monad_eth_getTransactionCount(reader, params)
                    .await
                    .map(serialize_result)?
            } else {
                Err(JsonRpcError::method_not_supported())
            }
        }
        "eth_blockNumber" => {
            if let Some(reader) = &app_state.triedb_reader {
                monad_eth_blockNumber(reader).await.map(serialize_result)?
            } else {
                Err(JsonRpcError::method_not_supported())
            }
        }
        "eth_chainId" => monad_eth_chainId(app_state.chain_id)
            .await
            .map(serialize_result)?,
        "eth_syncing" => monad_eth_syncing().await,
        "eth_estimateGas" => {
            let Some(triedb_env) = &app_state.triedb_reader else {
                return Err(JsonRpcError::method_not_supported());
            };

            let Some(execution_ledger_path) = &app_state.execution_ledger_path.0 else {
                debug!("execution ledger path was not set");
                return Err(JsonRpcError::method_not_supported());
            };

            // acquire the concurrent requests permit
            let _permit = &app_state.rate_limiter.try_acquire().map_err(|_| {
                JsonRpcError::internal_error("eth_estimateGas concurrent requests limit".into())
            })?;

            let params = serde_json::from_value(params).invalid_params()?;
            monad_eth_estimateGas(
                triedb_env,
                execution_ledger_path.as_path(),
                app_state.chain_id,
                params,
            )
            .await
            .map(serialize_result)?
        }
        "eth_gasPrice" => {
            if let Some(triedb_env) = &app_state.triedb_reader {
                monad_eth_gasPrice(triedb_env).await.map(serialize_result)?
            } else {
                Err(JsonRpcError::method_not_supported())
            }
        }
        "eth_maxPriorityFeePerGas" => {
            if let Some(triedb_env) = &app_state.triedb_reader {
                monad_eth_maxPriorityFeePerGas(triedb_env)
                    .await
                    .map(serialize_result)?
            } else {
                Err(JsonRpcError::method_not_supported())
            }
        }
        "eth_feeHistory" => {
            if let Some(triedb_env) = &app_state.triedb_reader {
                let params = serde_json::from_value(params).invalid_params()?;
                monad_eth_feeHistory(triedb_env, params)
                    .await
                    .map(serialize_result)?
            } else {
                Err(JsonRpcError::method_not_supported())
            }
        }
        "eth_getTransactionReceipt" => {
            let Some(triedb_reader) = &app_state.triedb_reader else {
                return Err(JsonRpcError::method_not_supported());
            };

            let params = serde_json::from_value(params).invalid_params()?;
            monad_eth_getTransactionReceipt(triedb_reader, params)
                .await
                .map(serialize_result)?
        }
        "eth_getBlockReceipts" => {
            let triedb_reader = app_state.triedb_reader.as_ref().method_not_supported()?;
            let params = serde_json::from_value(params).invalid_params()?;
            monad_eth_getBlockReceipts(triedb_reader, params)
                .await
                .map(serialize_result)?
        }
        "eth_getProof" => {
            let triedb_env = app_state.triedb_reader.as_ref().method_not_supported()?;
            let params = serde_json::from_value(params).invalid_params()?;
            monad_eth_getProof(triedb_env, params)
                .await
                .map(serialize_result)?
        }
        "eth_sendTransaction" => Err(JsonRpcError::method_not_supported()),
        "eth_signTransaction" => Err(JsonRpcError::method_not_supported()),
        "eth_sign" => Err(JsonRpcError::method_not_supported()),
        "eth_hashrate" => Err(JsonRpcError::method_not_supported()),
        "net_version" => serialize_result(app_state.chain_id.to_string()),
        "trace_block" => {
            let params = serde_json::from_value(params).invalid_params()?;
            monad_trace_block(params).await.map(serialize_result)?
        }
        "trace_call" => {
            let params = serde_json::from_value(params).invalid_params()?;
            monad_trace_call(params).await.map(serialize_result)?
        }
        "trace_callMany" => monad_trace_callMany().await.map(serialize_result)?,
        "trace_get" => {
            let params = serde_json::from_value(params).invalid_params()?;
            monad_trace_get(params).await.map(serialize_result)?
        }
        "trace_transaction" => {
            let params = serde_json::from_value(params).invalid_params()?;
            monad_trace_transaction(params)
                .await
                .map(serialize_result)?
        }
        "txpool_content" => monad_txpool_content().await.map(serialize_result)?,
        "txpool_contentFrom" => {
            let params = serde_json::from_value(params).invalid_params()?;
            monad_txpool_contentFrom(&app_state.tx_pool, params)
                .await
                .map(serialize_result)?
        }
        "txpool_inspect" => monad_txpool_inspect().await.map(serialize_result)?,
        "txpool_status" => monad_txpool_status(&app_state.tx_pool)
            .await
            .map(serialize_result)?,
        "web3_clientVersion" => serialize_result("monad"),
        _ => Err(JsonRpcError::method_not_found()),
    }
}

#[derive(Debug, Clone)]
struct ExecutionLedgerPath(pub Option<PathBuf>);

#[derive(Clone)]
struct MonadRpcResources {
    mempool_sender: flume::Sender<TransactionSigned>,
    triedb_reader: Option<TriedbEnv>,
    execution_ledger_path: ExecutionLedgerPath,
    chain_id: u64,
    batch_request_limit: u16,
    max_response_size: u32,
    allow_unprotected_txs: bool,
    rate_limiter: Arc<Semaphore>,
    tx_pool: Arc<vpool::VirtualPool>,
}

impl Handler<Disconnect> for MonadRpcResources {
    type Result = ();

    fn handle(&mut self, _msg: Disconnect, ctx: &mut Self::Context) -> Self::Result {
        debug!("received disconnect {:?}", ctx);
    }
}

impl MonadRpcResources {
    pub fn new(
        mempool_sender: flume::Sender<TransactionSigned>,
        triedb_reader: Option<TriedbEnv>,
        execution_ledger_path: Option<PathBuf>,
        chain_id: u64,
        batch_request_limit: u16,
        max_response_size: u32,
        allow_unprotected_txs: bool,
        rate_limiter: Arc<Semaphore>,
        tx_pool: Arc<vpool::VirtualPool>,
    ) -> Self {
        Self {
            mempool_sender,
            triedb_reader,
            execution_ledger_path: ExecutionLedgerPath(execution_ledger_path),
            chain_id,
            batch_request_limit,
            max_response_size,
            allow_unprotected_txs,
            rate_limiter,
            tx_pool,
        }
    }
}

impl Actor for MonadRpcResources {
    type Context = Context<Self>;
}

pub fn create_app_with_metrics<S: 'static>(
    app_data: S,
    with_metrics: metrics::Metrics,
) -> App<
    impl ServiceFactory<
        ServiceRequest,
        Config = (),
        Response = ServiceResponse,
        Error = actix_web::Error,
        InitError = (),
    >,
> {
    App::new()
        .app_data(web::JsonConfig::default().limit(8192))
        .app_data(web::Data::new(app_data))
        .wrap(with_metrics)
        .service(web::resource("/").route(web::post().to(rpc_handler)))
        .service(web::resource("/ws/").route(web::get().to(websocket::handler)))
}

pub fn create_app<S: 'static>(
    app_data: S,
) -> App<
    impl ServiceFactory<
        ServiceRequest,
        Response = ServiceResponse<BoxBody>,
        Config = (),
        InitError = (),
        Error = Error,
    >,
> {
    App::new()
        .app_data(web::JsonConfig::default().limit(8192))
        .app_data(web::Data::new(app_data))
        .service(web::resource("/").route(web::post().to(rpc_handler)))
        .service(web::resource("/ws/").route(web::get().to(websocket::handler)))
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args = Cli::parse();

    let subscriber = Registry::default()
        .with(EnvFilter::from_default_env())
        .with(
            FmtLayer::default()
                .json()
                .with_span_events(FmtSpan::NONE)
                .with_current_span(false)
                .with_span_list(false)
                .with_writer(std::io::stdout)
                .with_ansi(false),
        );

    tracing::subscriber::set_global_default(subscriber).expect("failed to set logger");

    // initialize concurrent requests limiter
    let concurrent_requests_limiter = Arc::new(Semaphore::new(
        args.eth_call_max_concurrent_requests as usize,
    ));

    // channels and thread for communicating over the mempool ipc socket
    // RPC handlers that need to send to the mempool can clone the ipc_sender
    // channel to send
    let (ipc_sender, ipc_receiver) = flume::bounded::<TransactionSigned>(
        // TODO configurable
        10_000,
    );
    tokio::spawn(async move {
        let ipc_path = args.ipc_path;
        let mut sender = retry(|| async { MempoolTxIpcSender::new(&ipc_path).await })
            .await
            .expect("failed to create ipc sender");

        while let Ok(tx) = ipc_receiver.recv_async().await {
            if let Err(e) = sender.send(tx).await {
                warn!("IPC send failed, monad-bft likely crashed: {}", e);
            }
        }
    });

    let tx_pool = Arc::new(vpool::VirtualPool::new(
        ipc_sender.clone(),
        args.vpool_capacity,
    ));

    let triedb_env = args
        .triedb_path
        .clone()
        .as_deref()
        .map(|path| TriedbEnv::new(path, args.triedb_max_concurrent_requests as usize));

    // We need to spawn a task to handle changes to the base fee, and block updates
    let tx_pool2 = tx_pool.clone();
    let triedb_env2 = triedb_env.clone();
    tokio::task::spawn(async move {
        let triedb_env = block_watcher::TrieDbBlockState::new(triedb_env2.unwrap());
        let mut watcher = block_watcher::BlockWatcher::new(triedb_env, 0);
        while let Some(block) = watcher.next().await {
            tx_pool2.new_block(block, 1_000).await;
        }
    });

    let resources = MonadRpcResources::new(
        ipc_sender.clone(),
        triedb_env,
        Some(args.execution_ledger_path),
        args.chain_id,
        args.batch_request_limit,
        args.max_response_size,
        args.allow_unprotected_txs,
        concurrent_requests_limiter,
        tx_pool,
    );

    let meter_provider: Option<opentelemetry_sdk::metrics::SdkMeterProvider> =
        args.otel_endpoint.map(|endpoint| {
            let provider = metrics::build_otel_meter_provider(
                &endpoint,
                "monad-rpc".to_string(),
                std::time::Duration::from_secs(5),
            )
            .expect("failed to build otel meter");
            opentelemetry::global::set_meter_provider(provider.clone());
            provider
        });

    let with_metrics = meter_provider
        .as_ref()
        .map(|provider| metrics::Metrics::new(provider.clone().meter("opentelemetry")));

    // main server app
    match with_metrics {
        Some(metrics) => {
            HttpServer::new(move || create_app_with_metrics(resources.clone(), metrics.clone()))
                .bind((args.rpc_addr, args.rpc_port))?
                .shutdown_timeout(1)
                .run()
        }
        None => HttpServer::new(move || create_app(resources.clone()))
            .bind((args.rpc_addr, args.rpc_port))?
            .shutdown_timeout(1)
            .run(),
    }
    .await?;

    Ok(())
}

async fn retry<T, E, F>(attempt: impl Fn() -> F) -> Result<T, E>
where
    F: futures::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let duration = std::time::Duration::from_secs(2);
    let mut retries = 1;

    loop {
        match attempt().await {
            Ok(t) => return Ok(t),
            Err(e) if retries <= 3 => {
                let timeout = duration * retries;
                debug!("caught error: {e}, retrying in {timeout:#?}");
                tokio::time::sleep(timeout).await;
                retries += 1;
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use actix_http::Request;
    use actix_web::{
        body::{to_bytes, MessageBody},
        dev::{Service, ServiceResponse},
        test, Error,
    };
    use alloy_consensus::{TxEip1559, TxLegacy};
    use alloy_primitives::{Address, TxKind, B256, U256};
    use alloy_rlp::Encodable;
    use reth_primitives::{sign_message, Transaction, TransactionSigned};
    use serde_json::{json, Number};
    use test_case::test_case;

    use super::*;

    pub struct MonadRpcResourcesState {
        pub ipc_receiver: flume::Receiver<TransactionSigned>,
    }

    pub async fn init_server() -> (
        impl Service<Request, Response = ServiceResponse<impl MessageBody>, Error = Error>,
        MonadRpcResourcesState,
    ) {
        let (ipc_sender, ipc_receiver) = flume::bounded::<TransactionSigned>(1_000);
        let m = MonadRpcResourcesState { ipc_receiver };
        let app = test::init_service(create_app(MonadRpcResources {
            mempool_sender: ipc_sender.clone(),
            triedb_reader: None,
            execution_ledger_path: ExecutionLedgerPath(None),
            chain_id: 1337,
            batch_request_limit: 5,
            max_response_size: 25_000_000,
            allow_unprotected_txs: false,
            rate_limiter: Arc::new(Semaphore::new(1000)),
            tx_pool: Arc::new(vpool::VirtualPool::new(ipc_sender.clone(), 20_000)),
        }))
        .await;
        (app, m)
    }

    fn make_tx_legacy(nonce: u64) -> (B256, String) {
        let input = vec![0; 64];
        let transaction = Transaction::Legacy(TxLegacy {
            chain_id: Some(1337),
            nonce,
            gas_price: 1000,
            gas_limit: 30000,
            to: TxKind::Call(Address::random()),
            value: U256::from(0),
            input: input.into(),
        });

        let hash = transaction.signature_hash();

        let sender_secret_key = B256::repeat_byte(0xcc);
        let signature =
            sign_message(sender_secret_key, hash).expect("signature should always succeed");
        let txn = TransactionSigned::new_unhashed(transaction, signature);

        let mut rlp_tx = Vec::new();
        txn.encode(&mut rlp_tx);
        (txn.hash(), hex::encode(&rlp_tx))
    }

    fn make_tx_eip1559(nonce: u64) -> (B256, String) {
        let input = vec![0; 64];
        let transaction = Transaction::Eip1559(TxEip1559 {
            chain_id: 1337,
            nonce,
            max_fee_per_gas: 1000,
            max_priority_fee_per_gas: 123,
            gas_limit: 30000,
            to: TxKind::Call(Address::random()),
            value: U256::from(0),
            input: input.into(),
            ..Default::default()
        });

        let hash = transaction.signature_hash();

        let sender_secret_key = B256::repeat_byte(0xcc);
        let signature =
            sign_message(sender_secret_key, hash).expect("signature should always succeed");
        let txn = TransactionSigned::new_unhashed(transaction, signature);

        let mut rlp_tx = Vec::new();
        txn.encode(&mut rlp_tx);
        (txn.hash(), hex::encode(&rlp_tx))
    }

    async fn recover_response_body(resp: ServiceResponse<impl MessageBody>) -> serde_json::Value {
        let b = to_bytes(resp.into_body())
            .await
            .unwrap_or_else(|_| panic!("body to_bytes failed"));
        serde_json::from_slice(&b)
            .inspect_err(|e| {
                println!("failed to serialize {:?}", &b);
            })
            .unwrap()
    }

    #[actix_web::test]
    async fn test_rpc_method_not_found() {
        let (app, _) = init_server().await;

        let payload = json!(
            {
                "jsonrpc": "2.0",
                "method": "subtract",
                "params": [42, 43],
                "id": 1
            }
        );
        let req = test::TestRequest::post()
            .uri("/")
            .set_payload(payload.to_string())
            .to_request();

        let resp = app.call(req).await.unwrap();
        let resp: jsonrpc::Response =
            serde_json::from_value(recover_response_body(resp).await).unwrap();

        match resp.error {
            Some(e) => assert_eq!(e.code, -32601),
            None => panic!("expected error in response"),
        }
    }

    #[allow(non_snake_case)]
    #[actix_web::test]
    async fn test_monad_eth_sendRawTransaction() {
        let (app, monad) = init_server().await;

        let test_input = [make_tx_legacy, make_tx_eip1559];
        for (i, f) in test_input.iter().enumerate() {
            let (expected_hash, rawtx) = f(i as u64);
            let payload = json!(
                {
                    "jsonrpc": "2.0",
                    "method": "eth_sendRawTransaction",
                    "params": [rawtx],
                    "id": 1
                }
            );

            let req = test::TestRequest::post()
                .uri("/")
                .set_payload(payload.to_string())
                .to_request();

            let resp = app.call(req).await.unwrap();
            let resp: jsonrpc::Response =
                serde_json::from_value(recover_response_body(resp).await).unwrap();

            match resp.result {
                Some(r) => assert_eq!(r, Value::String(expected_hash.to_string())),
                None => panic!("expected a result in response"),
            }

            let txn = monad
                .ipc_receiver
                .try_recv()
                .unwrap_or_else(|_| panic!("testcase {i}: nothing was sent on channel"));
            assert_eq!(expected_hash, txn.hash());
        }
    }

    #[allow(non_snake_case)]
    #[test_case(json!([]), ResponseWrapper::Single(Response::new(None, Some(JsonRpcError::invalid_request()), Value::Null)); "empty batch")]
    #[test_case(json!([1]), ResponseWrapper::Batch(vec![Response::new(None, Some(JsonRpcError::invalid_request()), Value::Null)]); "invalid batch but not empty")]
    #[test_case(json!([1, 2, 3, 4]),
    ResponseWrapper::Batch(vec![
        Response::new(None, Some(JsonRpcError::invalid_request()), Value::Null),
        Response::new(None, Some(JsonRpcError::invalid_request()), Value::Null),
        Response::new(None, Some(JsonRpcError::invalid_request()), Value::Null),
        Response::new(None, Some(JsonRpcError::invalid_request()), Value::Null),
    ]); "multiple invalid batch")]
    #[test_case(json!([
        {"jsonrpc": "2.0", "method": "subtract", "params": [42, 43], "id": 1},
        1,
        {"jsonrpc": "2.0", "method": "subtract", "params": [42, 43], "id": 1}
    ]),
    ResponseWrapper::Batch(
        vec![
            Response::new(None, Some(JsonRpcError::method_not_found()), Value::Number(Number::from(1))),
            Response::new(None, Some(JsonRpcError::invalid_request()), Value::Null),
            Response::new(None, Some(JsonRpcError::method_not_found()), Value::Number(Number::from(1))),
        ],
    ); "partial success")]
    #[test_case(json!([
        {"jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id": 1},
        {"jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id": 1},
        {"jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id": 1},
        {"jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id": 1},
        {"jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id": 1},
        {"jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id": 1}
    ]),
    ResponseWrapper::Single(Response::new(None, Some(JsonRpcError::invalid_request()), Value::Null)); "exceed batch request limit")]
    #[actix_web::test]
    async fn json_rpc_specification_batch_compliance(
        payload: Value,
        expected: ResponseWrapper<Response>,
    ) {
        let (app, _) = init_server().await;

        let req = test::TestRequest::post()
            .uri("/")
            .set_payload(payload.to_string())
            .to_request();

        let resp = app.call(req).await.unwrap();
        let resp: jsonrpc::ResponseWrapper<Response> =
            serde_json::from_value(recover_response_body(resp).await).unwrap();
        assert_eq!(resp, expected);
    }
}
