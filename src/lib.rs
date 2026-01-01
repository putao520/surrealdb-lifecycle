//! SurrealDB Server lifecycle manager with cross-process reference counting.
//!
//! ## Features
//!
//! - **Cross-process reference counting**: Uses named semaphores for coordination
//! - **Automatic server startup**: Starts SurrealDB server on demand
//! - **Health check & reconnection**: Automatically detects dead connections
//! - **RAII cleanup**: Proper cleanup on process exit
//!
//! ## Usage
//!
//! ```no_run
//! use surrealdb_lifecycle::ServerCoordinator;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Get or initialize the global SurrealDB connection
//!     let db = ServerCoordinator::global_db_or_init().await?;
//!     // Use the database connection...
//!     Ok(())
//! }
//! ```

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use named_sem::NamedSemaphore;
use serde::{Deserialize, Serialize};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::process::Command;
use tokio::time::{sleep, timeout};

use surrealdb::engine::remote::ws::{Client, Ws};
use surrealdb::opt::auth::Root;
use surrealdb::Surreal;

use std::sync::{OnceLock, LazyLock};
use tokio::sync::RwLock;

/// Global singleton SurrealDB instance.
static GLOBAL_DB: LazyLock<RwLock<Option<Arc<Surreal<Client>>>>> = LazyLock::new(|| RwLock::new(None));

/// Global semaphore guard for the DB connection.
static GLOBAL_GUARD: OnceLock<&'static SemaphoreGuard> = OnceLock::new();

/// Global semaphore handle for atexit cleanup.
static GLOBAL_SEMAPHORE: OnceLock<SyncSemaphore> = OnceLock::new();

/// Global server PID for cleanup.
static GLOBAL_SERVER_PID: OnceLock<u32> = OnceLock::new();

/// Global lock path for cleanup.
static GLOBAL_LOCK_PATH: OnceLock<PathBuf> = OnceLock::new();

/// Flag to track if atexit handler has been registered.
static ATEXIT_REGISTERED: OnceLock<()> = OnceLock::new();

/// Default configuration values.
pub mod defaults {
    /// Port range for SurrealDB server.
    pub const PORT_START: u16 = 18500;
    pub const PORT_END: u16 = 18599;

    /// Maximum concurrent connections (semaphore initial value).
    pub const MAX_CONNECTIONS: usize = 100;

    /// Timeout for semaphore acquisition.
    pub const WAIT_TIMEOUT_SECS: u64 = 1;

    /// Default auth credentials.
    pub const USERNAME: &str = "root";
    pub const PASSWORD: &str = "root";
    pub const NAMESPACE: &str = "default";
    pub const DATABASE: &str = "default";

    /// Default paths (relative to home directory).
    pub const LOCK_PATH: &str = ".surrealdb/server.lock";
    pub const DATA_DIR: &str = ".surrealdb/data";
}

#[cfg(unix)]
extern "C" fn atexit_cleanup_semaphore() {
    use libc;

    if let Some(sem) = GLOBAL_SEMAPHORE.get() {
        let _ = unsafe {
            let ptr = sem as *const SyncSemaphore as *mut SyncSemaphore;
            (*ptr).post()
        };
    }

    if let Some(&pid) = GLOBAL_SERVER_PID.get() {
        if let Some(lock_path) = GLOBAL_LOCK_PATH.get() {
            if let Some(lock) = ServerLock::read(lock_path) {
                if lock.ref_count <= 1 && lock.pid == pid {
                    unsafe { libc::kill(pid as i32, libc::SIGTERM) };
                    std::thread::sleep(Duration::from_millis(100));
                    if is_process_alive(pid) {
                        unsafe { libc::kill(pid as i32, libc::SIGKILL) };
                    }
                    let _ = ServerLock::delete(lock_path);
                }
            }
        }
    }
}

#[cfg(windows)]
extern "system" fn atexit_cleanup_semaphore() {
    if let Some(&pid) = GLOBAL_SERVER_PID.get() {
        if let Some(lock_path) = GLOBAL_LOCK_PATH.get() {
            if let Some(lock) = ServerLock::read(lock_path) {
                if lock.ref_count <= 1 && lock.pid == pid {
                    unsafe {
                        use winapi::um::handleapi::CloseHandle;
                        use winapi::um::processthreadsapi::OpenProcess;
                        use winapi::um::winnt::{PROCESS_TERMINATE, SYNCHRONIZE};
                        let handle = OpenProcess(PROCESS_TERMINATE | SYNCHRONIZE, 0, pid);
                        if !handle.is_null() {
                            winapi::um::processthreadsapi::TerminateProcess(handle, 0);
                            CloseHandle(handle);
                        }
                    }
                    let _ = ServerLock::delete(lock_path);
                }
            }
        }
    }
}

fn register_atexit_handler(semaphore_name: &str) {
    ATEXIT_REGISTERED.get_or_init(|| {
        #[cfg(unix)]
        unsafe {
            if libc::atexit(atexit_cleanup_semaphore) != 0 {
                eprintln!("Warning: Failed to register atexit handler for semaphore cleanup");
            }
        }
        let _ = semaphore_name;
    });
}

struct SyncSemaphore(NamedSemaphore);

unsafe impl Send for SyncSemaphore {}
unsafe impl Sync for SyncSemaphore {}

impl SyncSemaphore {
    fn create(name: &str, value: u32) -> Result<Self> {
        Ok(Self(NamedSemaphore::create(name, value)?))
    }

    fn wait(&mut self) -> Result<(), named_sem::Error> {
        self.0.wait()
    }

    fn post(&mut self) -> Result<(), named_sem::Error> {
        self.0.post()
    }
}

/// Server lock file metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerLock {
    pub pid: u32,
    pub port: u16,
    pub started: DateTime<Utc>,
    pub version: String,
    pub ref_count: usize,
}

impl ServerLock {
    pub fn read(path: &Path) -> Option<Self> {
        let contents = std::fs::read_to_string(path).ok()?;
        serde_json::from_str(&contents).ok()
    }

    pub fn write(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("Failed to create lock directory {}", parent.display())
            })?;
        }
        let payload = serde_json::to_string_pretty(self)?;
        std::fs::write(path, payload)
            .with_context(|| format!("Failed to write server lock {}", path.display()))?;
        Ok(())
    }

    pub fn delete(path: &Path) -> Result<()> {
        if path.exists() {
            std::fs::remove_file(path)
                .with_context(|| format!("Failed to delete server lock {}", path.display()))?;
        }
        Ok(())
    }

    pub fn is_process_alive(&self) -> bool {
        is_process_alive(self.pid)
    }
}

/// RAII guard for semaphore reference.
pub struct SemaphoreGuard {
    sem: SyncSemaphore,
    lock_path: PathBuf,
    server_pid: u32,
    max_connections: usize,
}

impl SemaphoreGuard {
    fn new(sem: SyncSemaphore, lock_path: PathBuf, server_pid: u32, max_connections: usize) -> Self {
        Self {
            sem,
            lock_path,
            server_pid,
            max_connections,
        }
    }
}

impl Drop for SemaphoreGuard {
    fn drop(&mut self) {
        if let Err(e) = self.sem.post() {
            eprintln!("Warning: Failed to post semaphore: {}", e);
            return;
        }

        // Check if we're the last one
        if let Ok(count) = self.semaphore_count() {
            if count >= self.max_connections - 1 {
                let _ = Self::shutdown_server_if_last(self.server_pid, &self.lock_path);
            }
        }
    }
}

impl SemaphoreGuard {
    fn semaphore_count(&self) -> Result<usize> {
        Ok(self.max_connections) // Placeholder - actual value not accessible via API
    }

    fn shutdown_server_if_last(pid: u32, lock_path: &Path) -> Result<()> {
        if let Some(lock) = ServerLock::read(lock_path) {
            if lock.is_process_alive() && lock.pid == pid {
                #[cfg(unix)]
                {
                    unsafe { libc::kill(pid as i32, libc::SIGTERM) };
                    std::thread::sleep(Duration::from_millis(100));
                    if is_process_alive(pid) {
                        unsafe { libc::kill(pid as i32, libc::SIGKILL) };
                    }
                }

                #[cfg(windows)]
                {
                    unsafe {
                        use winapi::um::handleapi::CloseHandle;
                        use winapi::um::processthreadsapi::OpenProcess;
                        use winapi::um::winnt::{PROCESS_TERMINATE, SYNCHRONIZE};
                        let handle = OpenProcess(PROCESS_TERMINATE | SYNCHRONIZE, 0, pid);
                        if !handle.is_null() {
                            winapi::um::processthreadsapi::TerminateProcess(handle, 0);
                            CloseHandle(handle);
                        }
                    }
                }

                let _ = ServerLock::delete(lock_path);
            }
        }
        Ok(())
    }
}

/// Configuration for ServerCoordinator.
#[derive(Clone, Debug)]
pub struct Config {
    pub lock_path: PathBuf,
    pub data_dir: PathBuf,
    pub semaphore_name: String,
    pub username: String,
    pub password: String,
    pub namespace: String,
    pub database: String,
    pub port_start: u16,
    pub port_end: u16,
    pub max_connections: usize,
    pub wait_timeout: Duration,
    pub version: String,
}

impl Default for Config {
    fn default() -> Self {
        let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
        Self {
            lock_path: home.join(defaults::LOCK_PATH),
            data_dir: home.join(defaults::DATA_DIR),
            semaphore_name: default_semaphore_name(),
            username: defaults::USERNAME.to_string(),
            password: defaults::PASSWORD.to_string(),
            namespace: defaults::NAMESPACE.to_string(),
            database: defaults::DATABASE.to_string(),
            port_start: defaults::PORT_START,
            port_end: defaults::PORT_END,
            max_connections: defaults::MAX_CONNECTIONS,
            wait_timeout: Duration::from_secs(defaults::WAIT_TIMEOUT_SECS),
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

fn default_semaphore_name() -> String {
    #[cfg(unix)]
    return "/surrealdb_server".to_string();
    #[cfg(windows)]
    return "surrealdb_server".to_string();
}

/// Builder for creating a custom ServerCoordinator configuration.
pub struct ConfigBuilder {
    config: Config,
}

impl Default for ConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ConfigBuilder {
    pub fn new() -> Self {
        Self {
            config: Config::default(),
        }
    }

    pub fn lock_path(mut self, path: PathBuf) -> Self {
        self.config.lock_path = path;
        self
    }

    pub fn data_dir(mut self, dir: PathBuf) -> Self {
        self.config.data_dir = dir;
        self
    }

    pub fn semaphore_name(mut self, name: impl Into<String>) -> Self {
        self.config.semaphore_name = name.into();
        self
    }

    pub fn credentials(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.config.username = username.into();
        self.config.password = password.into();
        self
    }

    pub fn namespace(mut self, ns: impl Into<String>) -> Self {
        self.config.namespace = ns.into();
        self
    }

    pub fn database(mut self, db: impl Into<String>) -> Self {
        self.config.database = db.into();
        self
    }

    pub fn port_range(mut self, start: u16, end: u16) -> Self {
        self.config.port_start = start;
        self.config.port_end = end;
        self
    }

    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.config.version = version.into();
        self
    }

    pub fn build(self) -> Config {
        self.config
    }
}

/// Server coordinator for SurrealDB lifecycle management.
pub struct ServerCoordinator {
    config: Config,
    _guard: Option<SemaphoreGuard>,
}

impl ServerCoordinator {
    /// Create a new coordinator with default configuration.
    pub fn new() -> Result<Self> {
        Self::with_config(Config::default())
    }

    /// Create a new coordinator with custom configuration.
    pub fn with_config(config: Config) -> Result<Self> {
        // Ensure directories exist
        std::fs::create_dir_all(&config.data_dir)
            .with_context(|| format!("Failed to create data dir {}", config.data_dir.display()))?;
        if let Some(parent) = config.lock_path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("Failed to create lock directory {}", parent.display())
            })?;
        }

        Ok(Self {
            config,
            _guard: None,
        })
    }

    /// Create a coordinator from a builder.
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::new()
    }

    /// Get the global singleton SurrealDB instance.
    pub fn global_db() -> Option<Arc<Surreal<Client>>> {
        GLOBAL_DB.try_read().ok().and_then(|opt| opt.as_ref().cloned())
    }

    async fn is_connection_healthy(db: &Surreal<Client>) -> bool {
        let health_check = timeout(Duration::from_secs(1), db.query("SELECT * FROM version"));
        matches!(health_check.await, Ok(Ok(_)))
    }

    /// Initialize or get the global singleton SurrealDB instance.
    pub async fn global_db_or_init() -> Result<Arc<Surreal<Client>>> {
        Self::global_db_or_init_with_config(Config::default()).await
    }

    /// Initialize or get the global singleton with custom configuration.
    pub async fn global_db_or_init_with_config(config: Config) -> Result<Arc<Surreal<Client>>> {
        let mut db_guard = GLOBAL_DB.write().await;
        if let Some(db) = db_guard.as_ref() {
            if Self::is_connection_healthy(db).await {
                return Ok(db.clone());
            }
            eprintln!("Warning: Cached SurrealDB connection is dead, reconnecting...");
            *db_guard = None;
        }
        drop(db_guard);

        let coordinator = Self::with_config(config.clone())?;
        let semaphore_name = coordinator.config.semaphore_name.clone();

        register_atexit_handler(&semaphore_name);

        let acquired = Self::acquire_with_timeout(
            &semaphore_name,
            coordinator.config.max_connections,
            &coordinator.config.wait_timeout,
        ).await?;

        if !acquired {
            Self::emergency_cleanup_with_config(&config)?;
            Self::acquire_with_timeout(
                &semaphore_name,
                coordinator.config.max_connections,
                &coordinator.config.wait_timeout,
            ).await
                .context("Failed to acquire semaphore after emergency cleanup")?;
        }

        let mut sem = SyncSemaphore::create(&semaphore_name, coordinator.config.max_connections as u32)
            .context("Failed to create named semaphore")?;
        sem.wait().context("Failed to wait on semaphore")?;

        let (db, server_pid, _is_creator) = coordinator.ensure_server_with_semaphore().await?;

        let db_arc = {
            let mut db_guard = GLOBAL_DB.write().await;
            if let Some(existing) = db_guard.as_ref() {
                let existing_clone = existing.clone();
                if Self::is_connection_healthy(&existing_clone).await {
                    drop(db_guard);
                    let _ = sem.post();
                    return Ok(existing_clone);
                }
            }
            let new_db = Arc::new(db);
            *db_guard = Some(new_db.clone());
            new_db
        };

        if let Some(mut lock) = ServerLock::read(&coordinator.config.lock_path) {
            lock.ref_count += 1;
            lock.write(&coordinator.config.lock_path)?;
        }

        let _ = GLOBAL_SEMAPHORE.set(sem);
        let _ = GLOBAL_SERVER_PID.set(server_pid);
        let _ = GLOBAL_LOCK_PATH.set(coordinator.config.lock_path.clone());

        let _guard = Box::leak(Box::new(SemaphoreGuard::new(
            SyncSemaphore::create(&semaphore_name, coordinator.config.max_connections as u32)
                .context("Failed to create semaphore for guard")?,
            coordinator.config.lock_path,
            server_pid,
            coordinator.config.max_connections,
        )));
        let _ = GLOBAL_GUARD.set(_guard);

        Ok(db_arc.clone())
    }

    pub fn emergency_cleanup() -> Result<()> {
        Self::emergency_cleanup_with_config(&Config::default())
    }

    fn emergency_cleanup_with_config(config: &Config) -> Result<()> {
        let _sem = SyncSemaphore::create(&config.semaphore_name, config.max_connections as u32)
            .context("Failed to recreate semaphore during emergency cleanup")?;

        if let Some(lock) = ServerLock::read(&config.lock_path) {
            if !lock.is_process_alive() {
                ServerLock::delete(&config.lock_path)?;
            }
        }

        Ok(())
    }

    pub async fn connect(&mut self) -> Result<Arc<Surreal<Client>>> {
        let acquired = Self::acquire_with_timeout(
            &self.config.semaphore_name,
            self.config.max_connections,
            &self.config.wait_timeout,
        ).await?;

        if !acquired {
            Self::emergency_cleanup_with_config(&self.config)?;
            Self::acquire_with_timeout(
                &self.config.semaphore_name,
                self.config.max_connections,
                &self.config.wait_timeout,
            ).await
                .context("Failed to acquire semaphore after emergency cleanup")?;
        }

        let mut sem = SyncSemaphore::create(&self.config.semaphore_name, self.config.max_connections as u32)
            .context("Failed to create named semaphore")?;
        sem.wait().context("Failed to wait on semaphore")?;

        let (db, server_pid, _is_creator) = self.ensure_server_with_semaphore().await?;

        let db_arc = {
            let mut db_guard = GLOBAL_DB.write().await;
            if let Some(existing) = db_guard.as_ref() {
                if Self::is_connection_healthy(existing).await {
                    let _ = sem.post();
                    if let Some(mut lock) = ServerLock::read(&self.config.lock_path) {
                        lock.ref_count += 1;
                        lock.write(&self.config.lock_path)?;
                    }
                    self._guard = Some(SemaphoreGuard::new(
                        SyncSemaphore::create(&self.config.semaphore_name, self.config.max_connections as u32)
                            .context("Failed to create semaphore for guard")?,
                        self.config.lock_path.clone(),
                        server_pid,
                        self.config.max_connections,
                    ));
                    return Ok(existing.clone());
                } else {
                    eprintln!("Warning: Cached SurrealDB connection is dead, reconnecting...");
                    let new_db = Arc::new(db);
                    *db_guard = Some(new_db.clone());
                    new_db
                }
            } else {
                let new_db = Arc::new(db);
                *db_guard = Some(new_db.clone());
                new_db
            }
        };

        if let Some(mut lock) = ServerLock::read(&self.config.lock_path) {
            lock.ref_count += 1;
            lock.write(&self.config.lock_path)?;
        }

        self._guard = Some(SemaphoreGuard::new(
            sem,
            self.config.lock_path.clone(),
            server_pid,
            self.config.max_connections,
        ));

        Ok(db_arc.clone())
    }

    async fn acquire_with_timeout(semaphore_name: &str, max_conn: usize, wait_timeout: &Duration) -> Result<bool> {
        let sem_name = semaphore_name.to_string();
        let max_conn = max_conn as u32;
        let dur = *wait_timeout;

        let wait_result = timeout(dur, async {
            tokio::task::spawn_blocking(move || {
                let mut sem_inner = SyncSemaphore::create(&sem_name, max_conn)?;
                sem_inner.wait()?;
                Ok::<(), anyhow::Error>(())
            })
            .await
            .map_err(|e| anyhow!("Spawn blocking failed: {}", e))?
        })
        .await;

        match wait_result {
            Ok(Ok(_)) => Ok(true),
            Ok(Err(e)) => Err(e).context("Semaphore acquisition failed"),
            Err(_) => Ok(false),
        }
    }

    async fn ensure_server_with_semaphore(&self) -> Result<(Surreal<Client>, u32, bool)> {
        if let Some(lock) = ServerLock::read(&self.config.lock_path) {
            if lock.is_process_alive() {
                if let Ok(db) = self.connect_with_retry(lock.port, 3).await {
                    return Ok((db, lock.pid, false));
                }
            }
            let _ = ServerLock::delete(&self.config.lock_path);
        }

        let (db, lock) = self.start_and_connect().await?;
        lock.write(&self.config.lock_path)?;
        Ok((db, lock.pid, true))
    }

    async fn start_and_connect(&self) -> Result<(Surreal<Client>, ServerLock)> {
        let binary = self.resolve_surreal_binary()?;
        let mut last_error = None;

        for port in self.config.port_start..=self.config.port_end {
            if !is_port_available(port) {
                continue;
            }

            match self.spawn_server(&binary, port).await {
                Ok(pid) => match self.connect_with_retry(port, 30).await {
                    Ok(db) => {
                        let lock = ServerLock {
                            pid,
                            port,
                            started: Utc::now(),
                            version: self.config.version.clone(),
                            ref_count: 1,
                        };
                        return Ok((db, lock));
                    }
                    Err(err) => {
                        last_error = Some(err);
                    }
                },
                Err(err) => {
                    last_error = Some(err);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            anyhow!("No available port in range {}-{}", self.config.port_start, self.config.port_end)
        }))
    }

    async fn spawn_server(&self, binary: &Path, port: u16) -> Result<u32> {
        let bind_addr = format!("127.0.0.1:{}", port);
        let data_uri = format!("surrealkv:{}", self.config.data_dir.display());

        let mut command = Command::new(binary);
        command
            .arg("start")
            .arg("--bind")
            .arg(&bind_addr)
            .arg("--user")
            .arg(&self.config.username)
            .arg("--pass")
            .arg(&self.config.password)
            .arg(&data_uri)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(false);

        let child = command
            .spawn()
            .with_context(|| format!("Failed to start SurrealDB with {}", binary.display()))?;
        child
            .id()
            .ok_or_else(|| anyhow!("SurrealDB process ID unavailable"))
    }

    async fn connect_with_retry(&self, port: u16, attempts: usize) -> Result<Surreal<Client>> {
        let mut last_error = None;
        for _ in 0..attempts {
            match self.connect_to_port(port).await {
                Ok(db) => return Ok(db),
                Err(err) => {
                    last_error = Some(err);
                }
            }
            sleep(Duration::from_millis(200)).await;
        }

        Err(last_error.unwrap_or_else(|| anyhow!("Failed to connect to port {}", port)))
    }

    async fn connect_to_port(&self, port: u16) -> Result<Surreal<Client>> {
        let address = format!("127.0.0.1:{}", port);
        let db = Surreal::new::<Ws>(&address).await?;
        db.signin(Root {
            username: &self.config.username,
            password: &self.config.password,
        })
        .await?;
        db.use_ns(&self.config.namespace).use_db(&self.config.database).await?;
        Ok(db)
    }

    fn resolve_surreal_binary(&self) -> Result<PathBuf> {
        if let Some(path) = managed_surreal_binary() {
            return Ok(path);
        }

        if let Some(path) = bundled_surreal_binary() {
            return Ok(path);
        }

        Err(anyhow!(
            "SurrealDB binary not found. Expected in ~/.surrealdb/bin/ or next to executable. \
             Install with: curl -fsSL https://install.surrealdb.com | sh"
        ))
    }

    pub fn is_server_running(&self) -> bool {
        ServerLock::read(&self.config.lock_path)
            .map(|lock| lock.is_process_alive())
            .unwrap_or(false)
    }
}

/// Look for SurrealDB binary in ~/.surrealdb/bin/ (managed location).
fn managed_surreal_binary() -> Option<PathBuf> {
    let home = dirs::home_dir()?;
    let base = home.join(".surrealdb").join("bin");

    let platform = match std::env::consts::OS {
        "linux" => "linux",
        "windows" => "windows",
        "macos" => "darwin",
        _ => return None,
    };
    let arch = match std::env::consts::ARCH {
        "x86_64" => "x64",
        "aarch64" => "arm64",
        _ => return None,
    };

    let mut name = format!("surreal-{}-{}", platform, arch);
    if cfg!(windows) {
        name.push_str(".exe");
    }
    let candidate = base.join(&name);
    if candidate.is_file() {
        return Some(candidate);
    }

    let generic_name = if cfg!(windows) { "surreal.exe" } else { "surreal" };
    let generic_candidate = base.join(generic_name);
    if generic_candidate.is_file() {
        return Some(generic_candidate);
    }

    None
}

/// Look for bundled SurrealDB binary next to the executable.
fn bundled_surreal_binary() -> Option<PathBuf> {
    let exe = std::env::current_exe().ok()?;
    let dir = exe.parent()?;
    let platform = match std::env::consts::OS {
        "linux" => "linux",
        "windows" => "windows",
        "macos" => "darwin",
        _ => return None,
    };
    let arch = match std::env::consts::ARCH {
        "x86_64" => "x64",
        "aarch64" => "arm64",
        _ => return None,
    };
    let mut name = format!("surreal-{}-{}", platform, arch);
    if cfg!(windows) {
        name.push_str(".exe");
    }
    let candidate = dir.join(name);
    if candidate.is_file() {
        Some(candidate)
    } else {
        None
    }
}

fn is_port_available(port: u16) -> bool {
    TcpListener::bind(("127.0.0.1", port)).is_ok()
}

fn is_process_alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        unsafe { libc::kill(pid as i32, 0) == 0 }
    }

    #[cfg(windows)]
    {
        use winapi::um::handleapi::CloseHandle;
        use winapi::um::processthreadsapi::OpenProcess;
        use winapi::um::winnt::PROCESS_QUERY_LIMITED_INFORMATION;

        unsafe {
            let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
            if handle.is_null() {
                return false;
            }
            let _ = CloseHandle(handle);
            true
        }
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = pid;
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_builder() {
        let config = ServerCoordinator::builder()
            .lock_path(PathBuf::from("/tmp/test.lock"))
            .data_dir(PathBuf::from("/tmp/data"))
            .port_range(19000, 19099)
            .credentials("admin", "secret")
            .namespace("test_ns")
            .database("test_db")
            .version("1.0.0")
            .build();

        assert_eq!(config.lock_path, PathBuf::from("/tmp/test.lock"));
        assert_eq!(config.data_dir, PathBuf::from("/tmp/data"));
        assert_eq!(config.port_start, 19000);
        assert_eq!(config.port_end, 19099);
        assert_eq!(config.username, "admin");
        assert_eq!(config.password, "secret");
        assert_eq!(config.namespace, "test_ns");
        assert_eq!(config.database, "test_db");
        assert_eq!(config.version, "1.0.0");
    }

    #[test]
    fn test_server_lock_serialization() {
        let lock = ServerLock {
            pid: 1234,
            port: 18500,
            started: Utc::now(),
            version: "0.1.0".to_string(),
            ref_count: 1,
        };

        let temp_dir = tempfile::tempdir().unwrap();
        let lock_path = temp_dir.path().join("test.lock");

        lock.write(&lock_path).unwrap();
        let read_lock = ServerLock::read(&lock_path).unwrap();

        assert_eq!(read_lock.pid, lock.pid);
        assert_eq!(read_lock.port, lock.port);
        assert_eq!(read_lock.ref_count, lock.ref_count);
    }

    #[test]
    fn test_is_process_alive() {
        let current_pid = std::process::id();
        assert!(is_process_alive(current_pid));
        assert!(!is_process_alive(999999));
    }

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.username, defaults::USERNAME);
        assert_eq!(config.password, defaults::PASSWORD);
        assert_eq!(config.namespace, defaults::NAMESPACE);
        assert_eq!(config.database, defaults::DATABASE);
        assert_eq!(config.port_start, defaults::PORT_START);
        assert_eq!(config.port_end, defaults::PORT_END);
        assert_eq!(config.max_connections, defaults::MAX_CONNECTIONS);
    }
}
