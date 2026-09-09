//! Finds installed agents. Never launches an agent and never touches the
//! network; login state is the adapter's to report at open.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::agent::{AgentId, AgentInstallation, InstallationSource, LoginMethod};
use crate::catalog::{AgentProfile, Upgrade};
use crate::event::{Diagnostic, DiagnosticLevel};
use crate::process::login_shell_path;
use crate::runtime::{DiscoveryReport, MissingAgent};

/// Scans every profile concurrently: env override, then the search dirs.
/// Report order follows the catalog.
pub(crate) async fn discover(profiles: &[AgentProfile]) -> DiscoveryReport {
    let mut report = DiscoveryReport {
        agents: Vec::new(),
        missing: Vec::new(),
        diagnostics: Vec::new(),
    };
    if profiles.is_empty() {
        return report;
    }
    let home = std::env::home_dir().unwrap_or_default();
    let path = std::env::var("PATH").ok();
    let login = login_shell_path().await;
    let scans = profiles
        .iter()
        .map(|profile| scan(profile, &home, path.as_deref(), login.as_deref()));
    for (found, diagnostics) in futures::future::join_all(scans).await {
        match found {
            Ok(agent) => report.agents.push(agent),
            Err(missing) => report.missing.push(missing),
        }
        report.diagnostics.extend(diagnostics);
    }
    report
}

/// One profile's scan: the installation or the missing record, plus any
/// diagnostics raised on the way. An installed upgrade wins over the base
/// CLI; a missing one rides the installation as `upgrade`.
async fn scan(
    profile: &AgentProfile,
    home: &Path,
    path: Option<&str>,
    login: Option<&str>,
) -> (Result<AgentInstallation, MissingAgent>, Vec<Diagnostic>) {
    let mut diagnostics = Vec::new();
    if let Some(exe) = env_override(profile, &mut diagnostics) {
        let mut agent = installation(profile, exe, InstallationSource::EnvOverride);
        // The override pins the base CLI, but a missing upgrade still rides
        // along as installation guidance.
        let dirs = search_dirs(profile, home, path, login);
        if let Some(upgrade) = &profile.upgrade {
            agent.upgrade = resolve_upgrade(profile, upgrade, &dirs, home).err();
        }
        return (Ok(agent), diagnostics);
    }
    let dirs = search_dirs(profile, home, path, login);
    let upgrade = profile
        .upgrade
        .as_ref()
        .map(|upgrade| resolve_upgrade(profile, upgrade, &dirs, home));
    let found = match (resolve(profile.cli, &dirs), upgrade) {
        (_, Some(Ok((exe, source, args)))) => {
            let mut agent = installation(profile, exe, source);
            agent.acp_args = Some(args);
            Ok(agent)
        }
        (Some((exe, source)), upgrade) => {
            let mut agent = installation(profile, exe, source);
            agent.upgrade = upgrade.and_then(Result::err);
            Ok(agent)
        }
        (None, _) => Err(MissingAgent {
            id: AgentId::new(profile.id),
            name: profile.name.into(),
            searched: dirs.into_iter().map(|(dir, _)| dir).collect(),
            install_hint: profile.install_hint.into(),
        }),
    };
    (found, diagnostics)
}

/// The upgrade's executable and ACP args, or the missing record naming it.
/// Searched in the base dirs plus its own extras.
#[allow(clippy::type_complexity)]
fn resolve_upgrade(
    profile: &AgentProfile,
    upgrade: &Upgrade,
    dirs: &[(PathBuf, InstallationSource)],
    home: &Path,
) -> Result<(PathBuf, InstallationSource, Vec<String>), MissingAgent> {
    let mut dirs = dirs.to_vec();
    dirs.extend(
        upgrade
            .extra_paths
            .iter()
            .map(|extra| (home.join(extra), InstallationSource::KnownLocation)),
    );
    match resolve(upgrade.cli, &dirs) {
        Some((exe, source)) => Ok((
            exe,
            source,
            upgrade.acp_args.iter().map(|a| (*a).to_owned()).collect(),
        )),
        None => Err(MissingAgent {
            id: AgentId::new(profile.id),
            name: upgrade.name.into(),
            searched: dirs.into_iter().map(|(dir, _)| dir).collect(),
            install_hint: upgrade.install_hint.into(),
        }),
    }
}

/// The executable named by the profile's env var, when set and valid.
fn env_override(profile: &AgentProfile, diagnostics: &mut Vec<Diagnostic>) -> Option<PathBuf> {
    let exe = PathBuf::from(std::env::var(profile.executable_env).ok()?);
    if is_executable(&exe) {
        return Some(exe);
    }
    diagnostics.push(Diagnostic {
        level: DiagnosticLevel::Warning,
        message: format!(
            "{} is set but {} is not an executable file",
            profile.executable_env,
            exe.display()
        ),
    });
    None
}

/// Where to look, in resolution order: own PATH, login-shell PATH,
/// version-manager bins, well-known locations, profile extras.
fn search_dirs(
    profile: &AgentProfile,
    home: &Path,
    path: Option<&str>,
    login_path: Option<&str>,
) -> Vec<(PathBuf, InstallationSource)> {
    let mut seen = std::collections::HashSet::new();
    let mut dirs = Vec::new();
    let mut add = |dir: PathBuf, source: InstallationSource| {
        if !dir.as_os_str().is_empty() && seen.insert(dir.clone()) {
            dirs.push((dir, source));
        }
    };
    for dir in split_path(path) {
        add(dir, InstallationSource::Path);
    }
    for dir in split_path(login_path) {
        add(dir, InstallationSource::LoginShellPath);
    }
    for dir in version_manager_dirs(home) {
        add(dir, InstallationSource::VersionManager);
    }
    for dir in ["/opt/homebrew/bin", "/usr/local/bin"] {
        add(PathBuf::from(dir), InstallationSource::KnownLocation);
    }
    for extra in profile.extra_paths {
        let dir = Path::new(extra);
        let dir = if dir.is_absolute() {
            dir.to_owned()
        } else {
            home.join(dir)
        };
        add(dir, InstallationSource::KnownLocation);
    }
    dirs
}

/// Suffixes an executable can carry, in preference order. Windows installs
/// are `.exe` (native) or `.cmd` / `.bat` shims (npm); the bare file there
/// is a bash shim that cannot be spawned.
#[cfg(unix)]
const EXE_SUFFIXES: &[&str] = &[""];
#[cfg(windows)]
const EXE_SUFFIXES: &[&str] = &[".exe", ".cmd", ".bat"];

/// First search dir that holds the executable.
fn resolve(
    cli: &str,
    dirs: &[(PathBuf, InstallationSource)],
) -> Option<(PathBuf, InstallationSource)> {
    dirs.iter().find_map(|(dir, source)| {
        EXE_SUFFIXES.iter().find_map(|suffix| {
            let exe = dir.join(format!("{cli}{suffix}"));
            is_executable(&exe).then(|| (exe, source.clone()))
        })
    })
}

/// Bin dirs of the common Node version managers, newest version first.
fn version_manager_dirs(home: &Path) -> Vec<PathBuf> {
    let mut dirs = vec![
        home.join(".volta/bin"),
        home.join(".bun/bin"),
        home.join(".local/share/pnpm"),
        home.join("Library/pnpm"),
        home.join(".npm-global/bin"),
    ];
    dirs.extend(versions_newest_first(
        &home.join(".nvm/versions/node"),
        "bin",
    ));
    dirs.extend(versions_newest_first(
        &home.join(".local/share/fnm/node-versions"),
        "installation/bin",
    ));
    dirs.extend(versions_newest_first(
        &home.join("Library/Application Support/fnm/node-versions"),
        "installation/bin",
    ));
    dirs
}

/// Version directories under `root`, newest first, each joined with `suffix`.
fn versions_newest_first(root: &Path, suffix: &str) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(root) else {
        return Vec::new();
    };
    let mut versions: Vec<(Vec<u64>, PathBuf)> = entries
        .flatten()
        .map(|e| (version_key(&e.file_name().to_string_lossy()), e.path()))
        .collect();
    versions.sort_by(|a, b| b.0.cmp(&a.0));
    versions
        .into_iter()
        .map(|(_, path)| path.join(suffix))
        .collect()
}

/// "v20.1.0" -> [20, 1, 0], for sorting.
fn version_key(name: &str) -> Vec<u64> {
    name.trim_start_matches('v')
        .split('.')
        .filter_map(|part| part.parse().ok())
        .collect()
}

/// The directories of a PATH string, empty ones included (callers filter).
fn split_path(path: Option<&str>) -> impl Iterator<Item = PathBuf> + '_ {
    std::env::split_paths(path.unwrap_or_default())
}

/// A regular file with an execute bit (any file on non-unix).
fn is_executable(path: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        path.metadata()
            .map(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
            .unwrap_or(false)
    }
    #[cfg(not(unix))]
    {
        path.is_file()
    }
}

/// One found executable as an installation.
fn installation(
    profile: &AgentProfile,
    executable: PathBuf,
    source: InstallationSource,
) -> AgentInstallation {
    AgentInstallation {
        id: AgentId::new(profile.id),
        name: profile.name.into(),
        executable_path: executable,
        source,
        upgrade: None,
        acp_args: None,
    }
}

/// The profile's login command plus one `EnvVar` method per documented key.
pub(crate) fn login_methods(profile: &AgentProfile, exe: &Path) -> Vec<LoginMethod> {
    let mut methods = Vec::new();
    if !profile.login_args.is_empty() {
        let mut command = vec![exe.to_string_lossy().into_owned()];
        command.extend(profile.login_args.iter().map(|a| a.to_string()));
        methods.push(LoginMethod::Terminal {
            description: format!("Run `{}` in a terminal", command.join(" ")),
            command,
            env: BTreeMap::new(),
        });
    }
    for var in profile.api_key_env {
        methods.push(LoginMethod::EnvVar {
            name: var.to_string(),
        });
    }
    methods
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::catalog::Connection;

    fn profile() -> AgentProfile {
        AgentProfile {
            id: "fake",
            name: "Fake",
            cli: "fake-agent",
            executable_env: "ANYAGENT_TEST_UNSET",
            config_home_env: None,
            connection: Connection::Acp { args: &[] },
            api_key_env: &["ANYAGENT_TEST_UNSET_KEY"],
            open_auth_kind: None,
            auth_error_hints: &[],
            login_args: &["login"],
            install_hint: "install fake",
            extra_paths: &["custom/bin"],
            upgrade: None,
        }
    }

    fn install(dir: &Path, name: &str) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        std::fs::create_dir_all(dir).unwrap();
        let exe = dir.join(name);
        std::fs::write(&exe, "#!/bin/sh\n").unwrap();
        std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o755)).unwrap();
        exe
    }

    /// search_dirs follows PATH > LoginShellPath > VersionManager > KnownLocation > extra_paths order.
    #[test]
    fn search_follows_the_resolution_order() {
        let home = Path::new("/h");
        let dirs = search_dirs(&profile(), home, Some("/a:/b"), Some("/b:/c"));
        let find = |path: &str| {
            dirs.iter()
                .position(|(dir, _)| dir == Path::new(path))
                .unwrap()
        };
        assert_eq!(dirs[0], (PathBuf::from("/a"), InstallationSource::Path));
        assert_eq!(dirs[1], (PathBuf::from("/b"), InstallationSource::Path));
        assert_eq!(
            dirs[2],
            (PathBuf::from("/c"), InstallationSource::LoginShellPath)
        );
        assert!(find("/h/.volta/bin") < find("/opt/homebrew/bin"));
        assert_eq!(
            *dirs.last().unwrap(),
            (
                PathBuf::from("/h/custom/bin"),
                InstallationSource::KnownLocation
            )
        );
    }

    /// Resolves newest version-manager install (v20.1.0 over v9.9.9).
    #[test]
    fn resolves_the_newest_version_manager_install() {
        let home = tempfile::tempdir().unwrap();
        install(
            &home.path().join(".nvm/versions/node/v9.9.9/bin"),
            "fake-agent",
        );
        let newest = install(
            &home.path().join(".nvm/versions/node/v20.1.0/bin"),
            "fake-agent",
        );
        let dirs = search_dirs(&profile(), home.path(), None, None);
        let (exe, source) = resolve("fake-agent", &dirs).unwrap();
        assert_eq!(exe, newest);
        assert_eq!(source, InstallationSource::VersionManager);
    }

    /// Login methods: the terminal command first, then one EnvVar per documented key.
    #[test]
    fn login_methods_list_the_command_then_the_keys() {
        let login = login_methods(&profile(), Path::new("/h/bin/fake-agent"));
        assert!(matches!(
            &login[0],
            LoginMethod::Terminal { command, .. } if command == &vec!["/h/bin/fake-agent".to_string(), "login".to_string()]
        ));
        assert!(matches!(
            &login[1],
            LoginMethod::EnvVar { name } if name == "ANYAGENT_TEST_UNSET_KEY"
        ));
        assert_eq!(login.len(), 2);
    }
}
