//! Finds installed agents and reads their login markers. Never launches an
//! agent and never touches the network.
//! Marker state is best effort because markers can be absent or stale.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::agent::{AgentId, AgentInstallation, AuthStatus, InstallationSource, LoginMethod};
use crate::catalog::{AgentProfile, AuthMarker};
use crate::event::{Diagnostic, DiagnosticLevel};
use crate::process::login_shell_path;
use crate::runtime::{DiscoveryReport, MissingAgent};

/// Scans every profile: env override, then the search dirs, then the login
/// markers of whatever was found.
pub(crate) async fn discover(profiles: &[AgentProfile]) -> DiscoveryReport {
    let mut report = DiscoveryReport {
        agents: Vec::new(),
        missing: Vec::new(),
        diagnostics: Vec::new(),
    };
    if profiles.is_empty() {
        return report;
    }
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_default();
    let path = std::env::var("PATH").ok();
    let login = login_shell_path().await;
    for profile in profiles {
        if let Some(exe) = env_override(profile, &mut report.diagnostics) {
            let agent = installation(profile, exe, InstallationSource::EnvOverride, &home).await;
            report.agents.push(agent);
            continue;
        }
        let dirs = search_dirs(profile, &home, path.as_deref(), login.as_deref());
        match resolve(profile.cli, &dirs) {
            Some((exe, source)) => {
                report
                    .agents
                    .push(installation(profile, exe, source, &home).await);
            }
            None => report.missing.push(MissingAgent {
                id: AgentId::new(profile.id),
                name: profile.name.into(),
                searched: dirs.into_iter().map(|(dir, _)| dir).collect(),
                install_hint: profile.install_hint.into(),
            }),
        }
    }
    report
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

/// First search dir that holds the executable.
fn resolve(
    cli: &str,
    dirs: &[(PathBuf, InstallationSource)],
) -> Option<(PathBuf, InstallationSource)> {
    dirs.iter().find_map(|(dir, source)| {
        let exe = dir.join(cli);
        is_executable(&exe).then(|| (exe, source.clone()))
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

fn split_path(path: Option<&str>) -> impl Iterator<Item = PathBuf> + '_ {
    path.unwrap_or_default()
        .split(':')
        .filter(|dir| !dir.is_empty())
        .map(PathBuf::from)
}

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

async fn installation(
    profile: &AgentProfile,
    executable: PathBuf,
    source: InstallationSource,
    home: &Path,
) -> AgentInstallation {
    AgentInstallation {
        id: AgentId::new(profile.id),
        name: profile.name.into(),
        auth: read_auth(profile, home, &executable).await,
        executable_path: executable,
        source,
        acp_args: None,
    }
}

/// Login state from offline markers (existence doesn't guarantee user is logged in)
async fn read_auth(profile: &AgentProfile, home: &Path, exe: &Path) -> Option<AuthStatus> {
    if profile.auth_markers.is_empty() {
        return None;
    }
    for marker in profile.auth_markers {
        let kind = match marker {
            AuthMarker::ConfigFile(rel, kind) if config_home(profile, home).join(rel).is_file() => {
                kind.clone()
            }
            AuthMarker::Keychain(service, kind) if keychain_present(service).await => kind.clone(),
            AuthMarker::ApiKeyEnv(var)
                if std::env::var(var).is_ok_and(|v| !v.trim().is_empty()) =>
            {
                crate::agent::AuthKind::ApiKey
            }
            _ => continue,
        };
        return Some(AuthStatus::Authenticated {
            kind,
            account: None,
        });
    }
    Some(AuthStatus::Unauthenticated {
        login: login_methods(profile, exe),
    })
}

/// The profile's login command plus one `EnvVar` method per API-key marker.
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
    for marker in profile.auth_markers {
        if let AuthMarker::ApiKeyEnv(var) = marker {
            methods.push(LoginMethod::EnvVar {
                name: var.to_string(),
            });
        }
    }
    methods
}

/// The agent's config directory: env override, else `~/<config_dir>`.
fn config_home(profile: &AgentProfile, home: &Path) -> PathBuf {
    profile
        .config_home_env
        .and_then(std::env::var_os)
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(profile.config_dir))
}

/// Presence check for a macOS keychain item; reads no secret, so no prompt.
#[cfg(target_os = "macos")]
async fn keychain_present(service: &str) -> bool {
    use std::process::Stdio;
    let status = tokio::process::Command::new("security")
        .args(["find-generic-password", "-s", service])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    tokio::time::timeout(std::time::Duration::from_secs(2), status)
        .await
        .map(|s| s.is_ok_and(|s| s.success()))
        .unwrap_or(false)
}

#[cfg(not(target_os = "macos"))]
async fn keychain_present(_service: &str) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::AuthKind;
    use crate::catalog::Connection;

    fn profile() -> AgentProfile {
        AgentProfile {
            id: "fake",
            name: "Fake",
            cli: "fake-agent",
            executable_env: "ANYAGENT_TEST_UNSET",
            config_dir: ".fake",
            config_home_env: None,
            connection: Connection::Acp { args: &[] },
            auth_markers: &[
                AuthMarker::ConfigFile("auth.json", AuthKind::Subscription),
                AuthMarker::ApiKeyEnv("ANYAGENT_TEST_UNSET_KEY"),
            ],
            login_args: &["login"],
            install_hint: "install fake",
            extra_paths: &["custom/bin"],
        }
    }

    #[cfg(unix)]
    fn install(dir: &Path, name: &str) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        std::fs::create_dir_all(dir).unwrap();
        let exe = dir.join(name);
        std::fs::write(&exe, "#!/bin/sh\n").unwrap();
        std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o755)).unwrap();
        exe
    }

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

    #[cfg(unix)]
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

    #[cfg(unix)]
    #[tokio::test]
    async fn auth_markers_decide_logged_in_or_out() {
        let home = tempfile::tempdir().unwrap();
        let exe = Path::new("/h/bin/fake-agent");

        let auth = read_auth(&profile(), home.path(), exe).await.unwrap();
        let AuthStatus::Unauthenticated { login } = auth else {
            panic!("no marker present means logged out");
        };
        assert!(matches!(
            &login[0],
            LoginMethod::Terminal { command, .. } if command == &vec!["/h/bin/fake-agent".to_string(), "login".to_string()]
        ));
        assert!(matches!(
            &login[1],
            LoginMethod::EnvVar { name } if name == "ANYAGENT_TEST_UNSET_KEY"
        ));

        std::fs::create_dir_all(home.path().join(".fake")).unwrap();
        std::fs::write(home.path().join(".fake/auth.json"), "{}").unwrap();
        let auth = read_auth(&profile(), home.path(), exe).await.unwrap();
        assert!(matches!(
            auth,
            AuthStatus::Authenticated {
                kind: AuthKind::Subscription,
                ..
            }
        ));
    }
}
