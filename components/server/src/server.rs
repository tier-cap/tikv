// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! This module startups all the components of a TiKV server.
//!
//! It is responsible for reading from configs, starting up the various server components,
//! and handling errors (mostly by aborting and reporting to the user).
//!
//! The entry point is `run_tikv`.
//!
//! Components are often used to initialize other components, and/or must be explicitly stopped.
//! We keep these components in the `TiKVServer` struct.

use std::{
    cmp,
    convert::TryFrom,
    env, fmt,
    fs::{self, File},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{atomic::AtomicU64, Arc, Mutex},
    time::{Duration, Instant},
};

use concurrency_manager::ConcurrencyManager;
use encryption_export::{data_key_manager_from_config, DataKeyManager};
use engine_rocks::{
    encryption::get_env as get_encrypted_env, file_system::get_env as get_inspected_env, util,
    RocksEngine,
};
use engine_traits::{
    compaction_job::CompactionJobInfo, Engines, RaftEngine, ALL_CFS, CF_DEFAULT, CF_WRITE,
};
use error_code::ErrorCodeExt;
use file_system::{
    set_io_rate_limiter, BytesFetcher, IORateLimiter, MetricsManager as IOMetricsManager,
};
use fs2::FileExt;
use futures::executor::block_on;
use grpcio::{EnvBuilder, Environment};
use kvproto::{
    backup::create_backup, cdcpb::create_change_data, deadlock::create_deadlock,
    debugpb::create_debug, diagnosticspb::create_diagnostics, import_sstpb::create_import_sst,
};
use pd_client::{PdClient, RpcClient};
use raft_log_engine::RaftLogEngine;
use raftstore::{
    coprocessor::{
        config::SplitCheckConfigManager, BoxConsistencyCheckObserver, ConsistencyCheckMethod,
        CoprocessorHost, RawConsistencyCheckObserver, RegionInfoAccessor,
    },
    router::ServerRaftStoreRouter,
    store::{
        config::RaftstoreConfigManager,
        fsm,
        fsm::store::{RaftBatchSystem, RaftRouter, StoreMeta, PENDING_MSG_CAP},
        snap, AutoSplitController, GlobalReplicationState, LocalReader, SnapManagerBuilder,
        SplitCheckRunner, SplitConfigManager, StoreMsg,
    },
};
use security::SecurityManager;
use tikv::{
    config::{ConfigController, DBConfigManger, DBType, TiKvConfig, DEFAULT_ROCKSDB_SUB_DIR},
    coprocessor, coprocessor_v2,
    import::{ImportSSTService, SSTImporter},
    read_pool::{build_yatp_read_pool, ReadPool},
    server::raftkv::ReplicaReadLockChecker,
    server::{
        config::Config as ServerConfig,
        config::ServerConfigManager,
        create_raft_storage,
        gc_worker::{AutoGcConfig, GcWorker},
        lock_manager::LockManager,
        resolve,
        service::{DebugService, DiagnosticsService},
        status_server::StatusServer,
        ttl::TTLChecker,
        Node, RaftKv, Server, CPU_CORES_QUOTA_GAUGE, DEFAULT_CLUSTER_ID, GRPC_THREAD_PREFIX,
    },
    storage::{self, config::StorageConfigManger, mvcc::MvccConsistencyCheckObserver, Engine},
};
use tikv_util::{
    check_environment_variables,
    config::{ensure_dir_exist, VersionTrack},
    sys::sys_quota::SysQuota,
    time::Monitor,
    worker::{Builder as WorkerBuilder, FutureWorker, LazyWorker, Worker},
};
use tokio::runtime::Builder;

use crate::raft_engine_switch::{check_and_dump_raft_db, check_and_dump_raft_engine};
use crate::{setup::*, signal_handler};

const GBSIZE: u64 = 1024 * 1024 * 1024;

/// Run a TiKV server. Returns when the server is shutdown by the user, in which
/// case the server will be properly stopped.
pub fn run_tikv(config: TiKvConfig) {
    // Sets the global logger ASAP.
    // It is okay to use the config w/o `validate()`,
    // because `initial_logger()` handles various conditions.
    initial_logger(&config);

    // Print version information.
    let build_timestamp = option_env!("TIKV_BUILD_TIME");
    tikv::log_tikv_info(build_timestamp);

    // Print resource quota.
    SysQuota::new().log_quota();
    CPU_CORES_QUOTA_GAUGE.set(SysQuota::new().cpu_cores_quota());

    // Do some prepare works before start.
    pre_start();

    let _m = Monitor::default();

    macro_rules! run_impl {
        ($ER: ty) => {{
            let mut tikv = TiKVServer::<$ER>::init(config);
            tikv.check_conflict_addr();
            tikv.init_fs();
            tikv.init_yatp();
            tikv.init_encryption();
            let (limiter, fetcher) = tikv.init_io_utility();
            let engines = tikv.init_raw_engines(Some(limiter.clone()));
            tikv.init_engines(engines.clone());
            let server_config = tikv.init_servers();
            tikv.register_services();
            tikv.init_metrics_flusher(fetcher);
            tikv.init_storage_stats_task(engines);
            tikv.run_server(server_config);
            tikv.run_status_server();

            signal_handler::wait_for_signal(Some(tikv.engines.take().unwrap().engines));
            tikv.stop();
        }};
    }

    if !config.raft_engine.enable {
        run_impl!(RocksEngine)
    } else {
        run_impl!(RaftLogEngine)
    }
}

const RESERVED_OPEN_FDS: u64 = 1000;

const DEFAULT_METRICS_FLUSH_INTERVAL: Duration = Duration::from_millis(10_000);
const DEFAULT_STORAGE_STATS_INTERVAL: Duration = Duration::from_secs(10);

/// A complete TiKV server.
struct TiKVServer<ER: RaftEngine> {
    config: TiKvConfig,
    cfg_controller: Option<ConfigController>,
    security_mgr: Arc<SecurityManager>,
    pd_client: Arc<RpcClient>,
    router: RaftRouter<RocksEngine, ER>,
    system: Option<RaftBatchSystem<RocksEngine, ER>>,
    resolver: resolve::PdStoreAddrResolver,
    state: Arc<Mutex<GlobalReplicationState>>,
    store_path: PathBuf,
    snap_path: PathBuf,
    //raft_path: Pathbuf, currently not impl.
    encryption_key_manager: Option<Arc<DataKeyManager>>,
    engines: Option<TiKVEngines<ER>>,
    servers: Option<Servers<ER>>,
    region_info_accessor: RegionInfoAccessor,
    coprocessor_host: Option<CoprocessorHost<RocksEngine>>,
    to_stop: Vec<Box<dyn Stop>>,
    lock_files: Vec<File>,
    concurrency_manager: ConcurrencyManager,
    env: Arc<Environment>,
    background_worker: Worker,
    write_permission: Arc<Mutex<bool>>,
}

struct TiKVEngines<ER: RaftEngine> {
    engines: Engines<RocksEngine, ER>,
    store_meta: Arc<Mutex<StoreMeta>>,
    engine: RaftKv<RocksEngine, ServerRaftStoreRouter<RocksEngine, ER>>,
}

struct Servers<ER: RaftEngine> {
    lock_mgr: LockManager,
    server: Server<RaftRouter<RocksEngine, ER>, resolve::PdStoreAddrResolver>,
    node: Node<RpcClient, ER>,
    importer: Arc<SSTImporter>,
    cdc_scheduler: tikv_util::worker::Scheduler<cdc::Task>,
}

impl<ER: RaftEngine> TiKVServer<ER> {
    fn init(mut config: TiKvConfig) -> TiKVServer<ER> {
        // It is okay use pd config and security config before `init_config`,
        // because these configs must be provided by command line, and only
        // used during startup process.
        let security_mgr = Arc::new(
            SecurityManager::new(&config.security)
                .unwrap_or_else(|e| fatal!("failed to create security manager: {}", e)),
        );
        let env = Arc::new(
            EnvBuilder::new()
                .cq_count(config.server.grpc_concurrency)
                .name_prefix(thd_name!(GRPC_THREAD_PREFIX))
                .build(),
        );
        let pd_client =
            Self::connect_to_pd_cluster(&mut config, env.clone(), Arc::clone(&security_mgr));

        // Initialize and check config
        let cfg_controller = Self::init_config(config);
        let config = cfg_controller.get_current();

        let store_path = Path::new(&config.storage.data_dir).to_owned();

        // Initialize raftstore channels.
        let (router, system) = fsm::create_raft_batch_system(&config.raft_store);

        let thread_count = config.server.background_thread_count;
        let background_worker = WorkerBuilder::new("background")
            .thread_count(thread_count)
            .create();
        let (resolver, state) =
            resolve::new_resolver(Arc::clone(&pd_client), &background_worker, router.clone());

        let mut coprocessor_host = Some(CoprocessorHost::new(
            router.clone(),
            config.coprocessor.clone(),
        ));
        let region_info_accessor = RegionInfoAccessor::new(coprocessor_host.as_mut().unwrap());

        // Initialize concurrency manager
        let latest_ts = block_on(pd_client.get_tso()).expect("failed to get timestamp from PD");
        let concurrency_manager = ConcurrencyManager::new(latest_ts);

        TiKVServer {
            config,
            cfg_controller: Some(cfg_controller),
            security_mgr,
            pd_client,
            router,
            system: Some(system),
            resolver,
            state,
            store_path: store_path.clone(),
            snap_path: store_path.join(Path::new("snap")),
            encryption_key_manager: None,
            engines: None,
            servers: None,
            region_info_accessor,
            coprocessor_host,
            to_stop: vec![],
            lock_files: vec![],
            concurrency_manager,
            env,
            background_worker,
            write_permission: Arc::new(Mutex::new(true)),
        }
    }

    /// Initialize and check the config
    ///
    /// Warnings are logged and fatal errors exist.
    ///
    /// #  Fatal errors
    ///
    /// - If `dynamic config` feature is enabled and failed to register config to PD
    /// - If some critical configs (like data dir) are differrent from last run
    /// - If the config can't pass `validate()`
    /// - If the max open file descriptor limit is not high enough to support
    ///   the main database and the raft database.
    fn init_config(mut config: TiKvConfig) -> ConfigController {
        validate_and_persist_config(&mut config, true);

        ensure_dir_exist(&config.storage.data_dir).unwrap();
        if !config.rocksdb.wal_dir.is_empty() {
            ensure_dir_exist(&config.rocksdb.wal_dir).unwrap();
        }
        if config.raft_engine.enable {
            ensure_dir_exist(&config.raft_engine.config().dir).unwrap();
        } else {
            ensure_dir_exist(&config.raft_store.raftdb_path).unwrap();
            if !config.raftdb.wal_dir.is_empty() {
                ensure_dir_exist(&config.raftdb.wal_dir).unwrap();
            }
        }

        check_system_config(&config);

        tikv_util::set_panic_hook(config.abort_on_panic, &config.storage.data_dir);

        info!(
            "using config";
            "config" => serde_json::to_string(&config).unwrap(),
        );
        if config.panic_when_unexpected_key_or_data {
            info!("panic-when-unexpected-key-or-data is on");
            tikv_util::set_panic_when_unexpected_key_or_data(true);
        }

        config.write_into_metrics();

        ConfigController::new(config)
    }

    fn connect_to_pd_cluster(
        config: &mut TiKvConfig,
        env: Arc<Environment>,
        security_mgr: Arc<SecurityManager>,
    ) -> Arc<RpcClient> {
        let pd_client = Arc::new(
            RpcClient::new(&config.pd, Some(env), security_mgr)
                .unwrap_or_else(|e| fatal!("failed to create rpc client: {}", e)),
        );

        let cluster_id = pd_client
            .get_cluster_id()
            .unwrap_or_else(|e| fatal!("failed to get cluster id: {}", e));
        if cluster_id == DEFAULT_CLUSTER_ID {
            fatal!("cluster id can't be {}", DEFAULT_CLUSTER_ID);
        }
        config.server.cluster_id = cluster_id;
        info!(
            "connect to PD cluster";
            "cluster_id" => cluster_id
        );

        pd_client
    }

    fn check_conflict_addr(&mut self) {
        let cur_addr: SocketAddr = self
            .config
            .server
            .addr
            .parse()
            .expect("failed to parse into a socket address");
        let cur_ip = cur_addr.ip();
        let cur_port = cur_addr.port();
        let lock_dir = get_lock_dir();

        let search_base = env::temp_dir().join(&lock_dir);
        std::fs::create_dir_all(&search_base)
            .unwrap_or_else(|_| panic!("create {} failed", search_base.display()));

        for entry in fs::read_dir(&search_base).unwrap().flatten() {
            if !entry.file_type().unwrap().is_file() {
                continue;
            }
            let file_path = entry.path();
            let file_name = file_path.file_name().unwrap().to_str().unwrap();
            if let Ok(addr) = file_name.replace('_', ":").parse::<SocketAddr>() {
                let ip = addr.ip();
                let port = addr.port();
                if cur_port == port
                    && (cur_ip == ip || cur_ip.is_unspecified() || ip.is_unspecified())
                {
                    let _ = try_lock_conflict_addr(file_path);
                }
            }
        }

        let cur_path = search_base.join(cur_addr.to_string().replace(':', "_"));
        let cur_file = try_lock_conflict_addr(cur_path);
        self.lock_files.push(cur_file);
    }

    fn init_fs(&mut self) {
        let lock_path = self.store_path.join(Path::new("LOCK"));

        let f = File::create(lock_path.as_path())
            .unwrap_or_else(|e| fatal!("failed to create lock at {}: {}", lock_path.display(), e));
        if f.try_lock_exclusive().is_err() {
            fatal!(
                "lock {} failed, maybe another instance is using this directory.",
                self.store_path.display()
            );
        }
        self.lock_files.push(f);

        if tikv_util::panic_mark_file_exists(&self.config.storage.data_dir) {
            fatal!(
                "panic_mark_file {} exists, there must be something wrong with the db. \
                     Do not remove the panic_mark_file and force the TiKV node to restart. \
                     Please contact TiKV maintainers to investigate the issue. \
                     If needed, use scale in and scale out to replace the TiKV node. \
                     https://docs.pingcap.com/tidb/stable/scale-tidb-using-tiup",
                tikv_util::panic_mark_file_path(&self.config.storage.data_dir).display()
            );
        }

        // We truncate a big file to make sure that both raftdb and kvdb of TiKV have enough space
        // to do compaction and region migration when TiKV recover. This file is created in
        // data_dir rather than db_path, because we must not increase store size of db_path.
        let disk_stats = fs2::statvfs(&self.config.storage.data_dir).unwrap();
        let mut capacity = disk_stats.total_space();
        if self.config.raft_store.capacity.0 > 0 {
            capacity = cmp::min(capacity, self.config.raft_store.capacity.0);
        }
        file_system::reserve_space_for_recover(
            &self.config.storage.data_dir,
            if self.config.storage.reserve_space.0 == 0 {
                0
            } else {
                // Max one of configured `reserve_space` and `storage.capacity * 5%`.
                cmp::max(
                    (capacity as f64 * 0.05) as u64,
                    self.config.storage.reserve_space.0,
                )
            },
        )
        .unwrap();
    }

    fn init_yatp(&self) {
        yatp::metrics::set_namespace(Some("tikv"));
        prometheus::register(Box::new(yatp::metrics::MULTILEVEL_LEVEL0_CHANCE.clone())).unwrap();
        prometheus::register(Box::new(yatp::metrics::MULTILEVEL_LEVEL_ELAPSED.clone())).unwrap();
    }

    fn init_encryption(&mut self) {
        self.encryption_key_manager = data_key_manager_from_config(
            &self.config.security.encryption,
            &self.config.storage.data_dir,
        )
        .map_err(|e| {
            panic!(
                "Encryption failed to initialize: {}. code: {}",
                e,
                e.error_code()
            )
        })
        .unwrap()
        .map(Arc::new);
    }

    fn create_raftstore_compaction_listener(&self) -> engine_rocks::CompactionListener {
        fn size_change_filter(info: &engine_rocks::RocksCompactionJobInfo) -> bool {
            // When calculating region size, we only consider write and default
            // column families.
            let cf = info.cf_name();
            if cf != CF_WRITE && cf != CF_DEFAULT {
                return false;
            }
            // Compactions in level 0 and level 1 are very frequently.
            if info.output_level() < 2 {
                return false;
            }

            true
        }

        let ch = Mutex::new(self.router.clone());
        let compacted_handler =
            Box::new(move |compacted_event: engine_rocks::RocksCompactedEvent| {
                let ch = ch.lock().unwrap();
                let event = StoreMsg::CompactedEvent(compacted_event);
                if let Err(e) = ch.send_control(event) {
                    error_unknown!(?e; "send compaction finished event to raftstore failed");
                }
            });
        engine_rocks::CompactionListener::new(compacted_handler, Some(size_change_filter))
    }

    fn init_engines(&mut self, engines: Engines<RocksEngine, ER>) {
        let store_meta = Arc::new(Mutex::new(StoreMeta::new(PENDING_MSG_CAP)));
        let engine = RaftKv::new(
            ServerRaftStoreRouter::new(
                self.router.clone(),
                LocalReader::new(engines.kv.clone(), store_meta.clone(), self.router.clone()),
            ),
            engines.kv.clone(),
        );

        self.engines = Some(TiKVEngines {
            engines,
            store_meta,
            engine,
        });
    }

    fn init_gc_worker(
        &mut self,
    ) -> GcWorker<
        RaftKv<RocksEngine, ServerRaftStoreRouter<RocksEngine, ER>>,
        RaftRouter<RocksEngine, ER>,
    > {
        let engines = self.engines.as_ref().unwrap();
        let mut gc_worker = GcWorker::new(
            engines.engine.clone(),
            self.router.clone(),
            self.config.gc.clone(),
            self.pd_client.feature_gate().clone(),
        );
        gc_worker
            .start()
            .unwrap_or_else(|e| fatal!("failed to start gc worker: {}", e));
        gc_worker
            .start_observe_lock_apply(
                self.coprocessor_host.as_mut().unwrap(),
                self.concurrency_manager.clone(),
            )
            .unwrap_or_else(|e| fatal!("gc worker failed to observe lock apply: {}", e));

        let cfg_controller = self.cfg_controller.as_mut().unwrap();
        cfg_controller.register(
            tikv::config::Module::Gc,
            Box::new(gc_worker.get_config_manager()),
        );

        gc_worker
    }

    fn init_servers(&mut self) -> Arc<VersionTrack<ServerConfig>> {
        let gc_worker = self.init_gc_worker();
        let mut ttl_checker = Box::new(LazyWorker::new("ttl-checker"));
        let ttl_scheduler = ttl_checker.scheduler();

        let cfg_controller = self.cfg_controller.as_mut().unwrap();
        cfg_controller.register(
            tikv::config::Module::Storage,
            Box::new(StorageConfigManger::new(
                self.engines.as_ref().unwrap().engine.kv_engine(),
                self.config.storage.block_cache.shared,
                ttl_scheduler,
            )),
        );

        // Create cdc.
        let mut cdc_worker = Box::new(LazyWorker::new("cdc"));
        let cdc_scheduler = cdc_worker.scheduler();
        let txn_extra_scheduler = cdc::CdcTxnExtraScheduler::new(cdc_scheduler.clone());

        self.engines
            .as_mut()
            .unwrap()
            .engine
            .set_txn_extra_scheduler(Arc::new(txn_extra_scheduler));

        let lock_mgr = LockManager::new(self.config.pessimistic_txn.pipelined);
        cfg_controller.register(
            tikv::config::Module::PessimisticTxn,
            Box::new(lock_mgr.config_manager()),
        );
        lock_mgr.register_detector_role_change_observer(self.coprocessor_host.as_mut().unwrap());

        let engines = self.engines.as_ref().unwrap();

        let pd_worker = FutureWorker::new("pd-worker");
        let pd_sender = pd_worker.scheduler();

        let unified_read_pool = if self.config.readpool.is_unified_pool_enabled() {
            Some(build_yatp_read_pool(
                &self.config.readpool.unified,
                pd_sender.clone(),
                engines.engine.clone(),
            ))
        } else {
            None
        };

        // The `DebugService` and `DiagnosticsService` will share the same thread pool
        let debug_thread_pool = Arc::new(
            Builder::new()
                .threaded_scheduler()
                .thread_name(thd_name!("debugger"))
                .core_threads(1)
                .on_thread_start(tikv_alloc::add_thread_memory_accessor)
                .on_thread_stop(tikv_alloc::remove_thread_memory_accessor)
                .build()
                .unwrap(),
        );

        let storage_read_pool_handle = if self.config.readpool.storage.use_unified_pool() {
            unified_read_pool.as_ref().unwrap().handle()
        } else {
            let storage_read_pools = ReadPool::from(storage::build_read_pool(
                &self.config.readpool.storage,
                pd_sender.clone(),
                engines.engine.clone(),
            ));
            storage_read_pools.handle()
        };

        let storage = create_raft_storage(
            engines.engine.clone(),
            &self.config.storage,
            storage_read_pool_handle,
            lock_mgr.clone(),
            self.concurrency_manager.clone(),
            lock_mgr.get_pipelined(),
        )
        .unwrap_or_else(|e| fatal!("failed to create raft storage: {}", e));

        ReplicaReadLockChecker::new(self.concurrency_manager.clone())
            .register(self.coprocessor_host.as_mut().unwrap());

        // Create snapshot manager, server.
        let snap_path = self.snap_path.clone().to_str().unwrap().to_owned();

        let bps = i64::try_from(self.config.server.snap_max_write_bytes_per_sec.0)
            .unwrap_or_else(|_| fatal!("snap_max_write_bytes_per_sec > i64::max_value"));

        let snap_mgr = SnapManagerBuilder::default()
            .max_write_bytes_per_sec(bps)
            .max_total_size(self.config.server.snap_max_total_size.0)
            .encryption_key_manager(self.encryption_key_manager.clone())
            .build(snap_path);

        // Create coprocessor endpoint.
        let cop_read_pool_handle = if self.config.readpool.coprocessor.use_unified_pool() {
            unified_read_pool.as_ref().unwrap().handle()
        } else {
            let cop_read_pools = ReadPool::from(coprocessor::readpool_impl::build_read_pool(
                &self.config.readpool.coprocessor,
                pd_sender,
                engines.engine.clone(),
            ));
            cop_read_pools.handle()
        };

        // Create resolved ts worker
        let mut rts_worker = Box::new(LazyWorker::new("resolved-ts"));
        let rts_scheduler = rts_worker.scheduler();

        // Register cdc
        let cdc_ob = cdc::CdcObserver::new(cdc_scheduler.clone());
        cdc_ob.register_to(self.coprocessor_host.as_mut().unwrap());
        // Register resolved ts
        let resolved_ts_ob = resolved_ts::Observer::new(rts_scheduler.clone());
        resolved_ts_ob.register_to(self.coprocessor_host.as_mut().unwrap());

        let server_config = Arc::new(VersionTrack::new(self.config.server.clone()));

        self.config
            .raft_store
            .validate()
            .unwrap_or_else(|e| fatal!("failed to validate raftstore config {}", e));
        let raft_store = Arc::new(VersionTrack::new(self.config.raft_store.clone()));
        let mut node = Node::new(
            self.system.take().unwrap(),
            &server_config.value().clone(),
            raft_store.clone(),
            self.pd_client.clone(),
            self.state.clone(),
            self.background_worker.clone(),
        );
        node.try_bootstrap_store(engines.engines.clone())
            .unwrap_or_else(|e| fatal!("failed to bootstrap node id: {}", e));

        // Create server
        let server = Server::new(
            node.id(),
            &server_config,
            &self.security_mgr,
            storage,
            coprocessor::Endpoint::new(
                &server_config.value(),
                cop_read_pool_handle,
                self.concurrency_manager.clone(),
                engine_rocks::raw_util::to_raw_perf_level(self.config.coprocessor.perf_level),
            ),
            coprocessor_v2::Endpoint::new(&self.config.coprocessor_v2),
            self.router.clone(),
            self.resolver.clone(),
            snap_mgr.clone(),
            gc_worker.clone(),
            self.env.clone(),
            unified_read_pool,
            debug_thread_pool,
        )
        .unwrap_or_else(|e| fatal!("failed to create server: {}", e));
        cfg_controller.register(
            tikv::config::Module::Server,
            Box::new(ServerConfigManager::new(
                server.get_snap_worker_scheduler(),
                server_config.clone(),
            )),
        );

        let import_path = self.store_path.join("import");
        let importer = Arc::new(
            SSTImporter::new(
                &self.config.import,
                import_path,
                self.encryption_key_manager.clone(),
            )
            .unwrap(),
        );

        let split_check_runner = SplitCheckRunner::new(
            engines.engines.kv.clone(),
            self.router.clone(),
            self.coprocessor_host.clone().unwrap(),
        );
        let split_check_scheduler = self
            .background_worker
            .start("split-check", split_check_runner);
        cfg_controller.register(
            tikv::config::Module::Coprocessor,
            Box::new(SplitCheckConfigManager(split_check_scheduler.clone())),
        );

        cfg_controller.register(
            tikv::config::Module::Raftstore,
            Box::new(RaftstoreConfigManager(raft_store)),
        );

        let split_config_manager =
            SplitConfigManager(Arc::new(VersionTrack::new(self.config.split.clone())));
        cfg_controller.register(
            tikv::config::Module::Split,
            Box::new(split_config_manager.clone()),
        );

        let auto_split_controller = AutoSplitController::new(split_config_manager);

        // `ConsistencyCheckObserver` must be registered before `Node::start`.
        let safe_point = Arc::new(AtomicU64::new(0));
        let observer = match self.config.coprocessor.consistency_check_method {
            ConsistencyCheckMethod::Mvcc => BoxConsistencyCheckObserver::new(
                MvccConsistencyCheckObserver::new(safe_point.clone()),
            ),
            ConsistencyCheckMethod::Raw => {
                BoxConsistencyCheckObserver::new(RawConsistencyCheckObserver::default())
            }
        };
        self.coprocessor_host
            .as_mut()
            .unwrap()
            .registry
            .register_consistency_check_observer(100, observer);

        node.start(
            engines.engines.clone(),
            server.transport(),
            snap_mgr,
            pd_worker,
            engines.store_meta.clone(),
            self.coprocessor_host.clone().unwrap(),
            importer.clone(),
            split_check_scheduler,
            auto_split_controller,
            self.concurrency_manager.clone(),
        )
        .unwrap_or_else(|e| fatal!("failed to start node: {}", e));

        // Start auto gc. Must after `Node::start` because `node_id` is initialized there.
        assert!(node.id() > 0); // Node id should never be 0.
        let auto_gc_config = AutoGcConfig::new(
            self.pd_client.clone(),
            self.region_info_accessor.clone(),
            node.id(),
        );
        if let Err(e) = gc_worker.start_auto_gc(auto_gc_config, safe_point) {
            fatal!("failed to start auto_gc on storage, error: {}", e);
        }

        initial_metric(&self.config.metric);
        if self.config.storage.enable_ttl {
            ttl_checker.start_with_timer(TTLChecker::new(
                self.engines.as_ref().unwrap().engine.kv_engine(),
                self.region_info_accessor.clone(),
                self.config.storage.ttl_check_poll_interval.into(),
            ));
            self.to_stop.push(ttl_checker);
        }

        // Start CDC.
        let cdc_endpoint = cdc::Endpoint::new(
            &self.config.cdc,
            self.pd_client.clone(),
            cdc_scheduler.clone(),
            self.router.clone(),
            cdc_ob,
            engines.store_meta.clone(),
            self.concurrency_manager.clone(),
            server.env(),
            self.security_mgr.clone(),
        );
        cdc_worker.start_with_timer(cdc_endpoint);
        self.to_stop.push(cdc_worker);
        // Start resolved ts
        let rts_endpoint = resolved_ts::Endpoint::new(
            &self.config.cdc,
            rts_scheduler,
            self.router.clone(),
            engines.store_meta.clone(),
            self.pd_client.clone(),
            self.concurrency_manager.clone(),
            server.env(),
            self.security_mgr.clone(),
            // TODO: replace to the cdc sinker
            resolved_ts::DummySinker::new(),
        );
        rts_worker.start(rts_endpoint);
        self.to_stop.push(rts_worker);

        self.servers = Some(Servers {
            lock_mgr,
            server,
            node,
            importer,
            cdc_scheduler,
        });

        server_config
    }

    fn register_services(&mut self) {
        let servers = self.servers.as_mut().unwrap();
        let engines = self.engines.as_ref().unwrap();

        // Import SST service.
        let import_service = ImportSSTService::new(
            self.config.import.clone(),
            self.router.clone(),
            engines.engines.kv.clone(),
            servers.importer.clone(),
        );
        if servers
            .server
            .register_service(create_import_sst(import_service))
            .is_some()
        {
            fatal!("failed to register import service");
        }

        // Debug service.
        let debug_service = DebugService::new(
            engines.engines.clone(),
            servers.server.get_debug_thread_pool().clone(),
            self.router.clone(),
            self.cfg_controller.as_ref().unwrap().clone(),
        );
        if servers
            .server
            .register_service(create_debug(debug_service))
            .is_some()
        {
            fatal!("failed to register debug service");
        }

        // Create Diagnostics service
        let diag_service = DiagnosticsService::new(
            servers.server.get_debug_thread_pool().clone(),
            self.config.log_file.clone(),
            self.config.slow_log_file.clone(),
        );
        if servers
            .server
            .register_service(create_diagnostics(diag_service))
            .is_some()
        {
            fatal!("failed to register diagnostics service");
        }

        // Lock manager.
        if servers
            .server
            .register_service(create_deadlock(servers.lock_mgr.deadlock_service()))
            .is_some()
        {
            fatal!("failed to register deadlock service");
        }

        servers
            .lock_mgr
            .start(
                servers.node.id(),
                self.pd_client.clone(),
                self.resolver.clone(),
                self.security_mgr.clone(),
                &self.config.pessimistic_txn,
            )
            .unwrap_or_else(|e| fatal!("failed to start lock manager: {}", e));

        // Backup service.
        let mut backup_worker = Box::new(self.background_worker.lazy_build("backup-endpoint"));
        let backup_scheduler = backup_worker.scheduler();
        let backup_service = backup::Service::new(backup_scheduler);
        if servers
            .server
            .register_service(create_backup(backup_service))
            .is_some()
        {
            fatal!("failed to register backup service");
        }

        let backup_endpoint = backup::Endpoint::new(
            servers.node.id(),
            engines.engine.clone(),
            self.region_info_accessor.clone(),
            engines.engines.kv.as_inner().clone(),
            self.config.backup.clone(),
            self.concurrency_manager.clone(),
        );
        self.cfg_controller.as_mut().unwrap().register(
            tikv::config::Module::Backup,
            Box::new(backup_endpoint.get_config_manager()),
        );
        backup_worker.start_with_timer(backup_endpoint);

        let cdc_service = cdc::Service::new(servers.cdc_scheduler.clone());
        if servers
            .server
            .register_service(create_change_data(cdc_service))
            .is_some()
        {
            fatal!("failed to register cdc service");
        }
    }

    fn init_io_utility(&mut self) -> (Arc<IORateLimiter>, BytesFetcher) {
        let io_snooper_on = self.config.enable_io_snoop
            && file_system::init_io_snooper()
                .map_err(|e| error_unknown!(%e; "failed to init io snooper"))
                .is_ok();
        let limiter = Arc::new(IORateLimiter::new(
            0,
            !io_snooper_on, /*enable_statistics*/
        ));
        let fetcher = if io_snooper_on {
            BytesFetcher::FromIOSnooper()
        } else {
            BytesFetcher::FromRateLimiter(limiter.statistics().unwrap())
        };
        // Set up IO limiter even when rate limit is disabled, so that rate limits can be
        // dynamically applied later on.
        set_io_rate_limiter(Some(limiter.clone()));
        (limiter, fetcher)
    }

    fn init_metrics_flusher(&mut self, fetcher: BytesFetcher) {
        let mut engine_metrics =
            EngineMetricsManager::new(self.engines.as_ref().unwrap().engines.clone());
        let mut io_metrics = IOMetricsManager::new(fetcher);
        let mut last_call = Instant::now();
        self.background_worker
            .spawn_interval_task(DEFAULT_METRICS_FLUSH_INTERVAL, move || {
                let now = Instant::now();
                let duration = now - last_call;
                last_call = now;
                engine_metrics.flush(duration);
                io_metrics.flush(duration);
            });
    }
    fn init_storage_stats_task(&self, engines: Engines<RocksEngine, ER>) {
        let flag = self.write_permission.clone();
        let config_disk_capacity: u64 = self.config.raft_store.capacity.0;
        let store_path = self.store_path.clone();
        let snap_path = self.snap_path.clone();
        self.background_worker
            .spawn_interval_task(DEFAULT_STORAGE_STATS_INTERVAL, move || {
                let disk_stats = match fs2::statvfs(&store_path) {
                    Err(e) => {
                        error!(
                            "get disk stat for kv store failed";
                            "kv path"=>store_path.to_str(),
                            "err"=>?e
                        );
                        return;
                    }
                    Ok(stats) => stats,
                };
                let disk_cap = disk_stats.total_space();
                let disk_available = disk_stats.available_space();
                let snap_size = (|| -> Option<u64> {
                    let mut total_size = 0;
                    for entry in fs::read_dir(&snap_path).unwrap() {
                        let (entry, metadata) =
                            match entry.and_then(|e| e.metadata().map(|m| (e, m))) {
                                Ok((e, m)) => (e, m),
                                Err(_e) => return Some(0), //TODO Fixit?
                            };

                        let path = entry.path();
                        let path_s = path.to_str().unwrap();
                        // Ignore cloned files as they are hard links on posix systems.
                        if !metadata.is_file()
                            || path_s.ends_with(snap::CLONE_FILE_SUFFIX)
                            || path_s.ends_with(snap::META_FILE_SUFFIX)
                        {
                            continue;
                        }
                        total_size += metadata.len();
                    }
                    Some(total_size)
                })()
                .unwrap();
                let kv_size: u64 = (|| -> u64 {
                    let mut used_size: u64 = 0;
                    for cf in ALL_CFS {
                        let handle = util::get_cf_handle(engines.kv.as_inner(), cf).unwrap();
                        used_size += util::get_engine_cf_used_size(engines.kv.as_inner(), handle);
                    }
                    used_size
                })();
                let total_used = snap_size + kv_size;
                let capacity = if config_disk_capacity == 0 || disk_cap < config_disk_capacity {
                    disk_cap
                } else {
                    config_disk_capacity
                };
                if total_used * 100 / capacity >= 95 {
                    warn!(
                        "disk usage threshold：total used {:?}, config cap={:?}, disk available={:?}",
                        total_used / GBSIZE,
                        config_disk_capacity / GBSIZE,
                        disk_available/GBSIZE
                    );
                    let mut f = flag.lock().unwrap();
                    *f = false;
                } else {
                    let mut f = flag.lock().unwrap();
                    *f = true;
                }

                warn!(
                    "disk capacity checking, disk capacity={:?},kv_size={:?},snap_size={:?},config-cap={:?},flag{:?}",
                    disk_cap, kv_size,   snap_size,config_disk_capacity,flag
                );
            })
    }

    fn run_server(&mut self, server_config: Arc<VersionTrack<ServerConfig>>) {
        let server = self.servers.as_mut().unwrap();
        server
            .server
            .build_and_bind()
            .unwrap_or_else(|e| fatal!("failed to build server: {}", e));
        server
            .server
            .start(server_config, self.security_mgr.clone())
            .unwrap_or_else(|e| fatal!("failed to start server: {}", e));
    }

    fn run_status_server(&mut self) {
        // Create a status server.
        let status_enabled = !self.config.server.status_addr.is_empty();
        if status_enabled {
            let mut status_server = match StatusServer::new(
                self.config.server.status_thread_pool_size,
                Some(self.pd_client.clone()),
                self.cfg_controller.take().unwrap(),
                Arc::new(self.config.security.clone()),
                self.router.clone(),
            ) {
                Ok(status_server) => Box::new(status_server),
                Err(e) => {
                    error_unknown!(%e; "failed to start runtime for status service");
                    return;
                }
            };
            // Start the status server.
            if let Err(e) = status_server.start(
                self.config.server.status_addr.clone(),
                self.config.server.advertise_status_addr.clone(),
            ) {
                error_unknown!(%e; "failed to bind addr for status service");
            } else {
                self.to_stop.push(status_server);
            }
        }
    }

    fn stop(self) {
        let mut servers = self.servers.unwrap();
        servers
            .server
            .stop()
            .unwrap_or_else(|e| fatal!("failed to stop server: {}", e));

        servers.node.stop();
        self.region_info_accessor.stop();

        servers.lock_mgr.stop();

        self.to_stop.into_iter().for_each(|s| s.stop());
    }
}

impl TiKVServer<RocksEngine> {
    fn init_raw_engines(
        &mut self,
        limiter: Option<Arc<IORateLimiter>>,
    ) -> Engines<RocksEngine, RocksEngine> {
        let env =
            get_encrypted_env(self.encryption_key_manager.clone(), None /*base_env*/).unwrap();
        let env = get_inspected_env(Some(env), limiter).unwrap();
        let block_cache = self.config.storage.block_cache.build_shared_cache();

        // Create raft engine.
        let raft_db_path = Path::new(&self.config.raft_store.raftdb_path);
        let config_raftdb = &self.config.raftdb;
        let mut raft_db_opts = config_raftdb.build_opt();
        raft_db_opts.set_env(env.clone());
        let raft_db_cf_opts = config_raftdb.build_cf_opts(&block_cache);
        let raft_engine = engine_rocks::raw_util::new_engine_opt(
            raft_db_path.to_str().unwrap(),
            raft_db_opts,
            raft_db_cf_opts,
        )
        .unwrap_or_else(|s| fatal!("failed to create raft engine: {}", s));

        // Create kv engine.
        let mut kv_db_opts = self.config.rocksdb.build_opt();
        kv_db_opts.set_env(env);
        kv_db_opts.add_event_listener(self.create_raftstore_compaction_listener());
        let kv_cfs_opts = self.config.rocksdb.build_cf_opts(
            &block_cache,
            Some(&self.region_info_accessor),
            self.config.storage.enable_ttl,
        );
        let db_path = self.store_path.join(Path::new(DEFAULT_ROCKSDB_SUB_DIR));
        let kv_engine = engine_rocks::raw_util::new_engine_opt(
            db_path.to_str().unwrap(),
            kv_db_opts,
            kv_cfs_opts,
        )
        .unwrap_or_else(|s| fatal!("failed to create kv engine: {}", s));

        let mut kv_engine = RocksEngine::from_db(Arc::new(kv_engine));
        let mut raft_engine = RocksEngine::from_db(Arc::new(raft_engine));
        let shared_block_cache = block_cache.is_some();
        kv_engine.set_shared_block_cache(shared_block_cache);
        raft_engine.set_shared_block_cache(shared_block_cache);
        let engines = Engines::new(kv_engine, raft_engine);

        check_and_dump_raft_engine(&self.config, &engines.raft, 8);

        let cfg_controller = self.cfg_controller.as_mut().unwrap();
        cfg_controller.register(
            tikv::config::Module::Rocksdb,
            Box::new(DBConfigManger::new(
                engines.kv.clone(),
                DBType::Kv,
                self.config.storage.block_cache.shared,
            )),
        );
        cfg_controller.register(
            tikv::config::Module::Raftdb,
            Box::new(DBConfigManger::new(
                engines.raft.clone(),
                DBType::Raft,
                self.config.storage.block_cache.shared,
            )),
        );

        engines
    }
}

impl TiKVServer<RaftLogEngine> {
    fn init_raw_engines(
        &mut self,
        limiter: Option<Arc<IORateLimiter>>,
    ) -> Engines<RocksEngine, RaftLogEngine> {
        let env =
            get_encrypted_env(self.encryption_key_manager.clone(), None /*base_env*/).unwrap();
        let env = get_inspected_env(Some(env), limiter).unwrap();
        let block_cache = self.config.storage.block_cache.build_shared_cache();

        // Create raft engine.
        let raft_config = self.config.raft_engine.config();
        let raft_engine = RaftLogEngine::new(raft_config);

        // Try to dump and recover raft data.
        check_and_dump_raft_db(&self.config, &raft_engine, &env, 8);

        // Create kv engine.
        let mut kv_db_opts = self.config.rocksdb.build_opt();
        kv_db_opts.set_env(env);
        kv_db_opts.add_event_listener(self.create_raftstore_compaction_listener());
        let kv_cfs_opts = self.config.rocksdb.build_cf_opts(
            &block_cache,
            Some(&self.region_info_accessor),
            self.config.storage.enable_ttl,
        );
        let db_path = self.store_path.join(Path::new(DEFAULT_ROCKSDB_SUB_DIR));
        let kv_engine = engine_rocks::raw_util::new_engine_opt(
            db_path.to_str().unwrap(),
            kv_db_opts,
            kv_cfs_opts,
        )
        .unwrap_or_else(|s| fatal!("failed to create kv engine: {}", s));

        let mut kv_engine = RocksEngine::from_db(Arc::new(kv_engine));
        let shared_block_cache = block_cache.is_some();
        kv_engine.set_shared_block_cache(shared_block_cache);
        let engines = Engines::new(kv_engine, raft_engine);

        let cfg_controller = self.cfg_controller.as_mut().unwrap();
        cfg_controller.register(
            tikv::config::Module::Rocksdb,
            Box::new(DBConfigManger::new(
                engines.kv.clone(),
                DBType::Kv,
                self.config.storage.block_cache.shared,
            )),
        );

        engines
    }
}

/// Various sanity-checks and logging before running a server.
///
/// Warnings are logged.
///
/// # Logs
///
/// The presence of these environment variables that affect the database
/// behavior is logged.
///
/// - `GRPC_POLL_STRATEGY`
/// - `http_proxy` and `https_proxy`
///
/// # Warnings
///
/// - if `net.core.somaxconn` < 32768
/// - if `net.ipv4.tcp_syncookies` is not 0
/// - if `vm.swappiness` is not 0
/// - if data directories are not on SSDs
/// - if the "TZ" environment variable is not set on unix
fn pre_start() {
    check_environment_variables();
    for e in tikv_util::config::check_kernel() {
        warn!(
            "check: kernel";
            "err" => %e
        );
    }
}

fn check_system_config(config: &TiKvConfig) {
    info!("beginning system configuration check");
    let mut rocksdb_max_open_files = config.rocksdb.max_open_files;
    if config.rocksdb.titan.enabled {
        // Titan engine maintains yet another pool of blob files and uses the same max
        // number of open files setup as rocksdb does. So we double the max required
        // open files here
        rocksdb_max_open_files *= 2;
    }
    if let Err(e) = tikv_util::config::check_max_open_fds(
        RESERVED_OPEN_FDS + (rocksdb_max_open_files + config.raftdb.max_open_files) as u64,
    ) {
        fatal!("{}", e);
    }

    // Check RocksDB data dir
    if let Err(e) = tikv_util::config::check_data_dir(&config.storage.data_dir) {
        warn!(
            "check: rocksdb-data-dir";
            "path" => &config.storage.data_dir,
            "err" => %e
        );
    }
    // Check raft data dir
    if let Err(e) = tikv_util::config::check_data_dir(&config.raft_store.raftdb_path) {
        warn!(
            "check: raftdb-path";
            "path" => &config.raft_store.raftdb_path,
            "err" => %e
        );
    }
}

fn try_lock_conflict_addr<P: AsRef<Path>>(path: P) -> File {
    let f = File::create(path.as_ref()).unwrap_or_else(|e| {
        fatal!(
            "failed to create lock at {}: {}",
            path.as_ref().display(),
            e
        )
    });

    if f.try_lock_exclusive().is_err() {
        fatal!(
            "{} already in use, maybe another instance is binding with this address.",
            path.as_ref().file_name().unwrap().to_str().unwrap()
        );
    }
    f
}

#[cfg(unix)]
fn get_lock_dir() -> String {
    format!("{}_TIKV_LOCK_FILES", unsafe { libc::getuid() })
}

#[cfg(not(unix))]
fn get_lock_dir() -> String {
    "TIKV_LOCK_FILES".to_owned()
}

/// A small trait for components which can be trivially stopped. Lets us keep
/// a list of these in `TiKV`, rather than storing each component individually.
trait Stop {
    fn stop(self: Box<Self>);
}

impl<E, R> Stop for StatusServer<E, R>
where
    E: 'static,
    R: 'static + Send,
{
    fn stop(self: Box<Self>) {
        (*self).stop()
    }
}

impl Stop for Worker {
    fn stop(self: Box<Self>) {
        Worker::stop(&self);
    }
}

impl<T: fmt::Display + Send + 'static> Stop for LazyWorker<T> {
    fn stop(self: Box<Self>) {
        self.stop_worker();
    }
}

const DEFAULT_ENGINE_METRICS_RESET_INTERVAL: Duration = Duration::from_millis(60_000);

pub struct EngineMetricsManager<R: RaftEngine> {
    engines: Engines<RocksEngine, R>,
}

impl<R: RaftEngine> EngineMetricsManager<R> {
    pub fn new(engines: Engines<RocksEngine, R>) -> Self {
        EngineMetricsManager { engines }
    }

    pub fn flush(&mut self, duration: Duration) {
        self.engines.kv.flush_metrics("kv");
        self.engines.raft.flush_metrics("raft");
        if duration >= DEFAULT_ENGINE_METRICS_RESET_INTERVAL {
            self.engines.kv.reset_statistics();
            self.engines.raft.reset_statistics();
        }
    }
}
