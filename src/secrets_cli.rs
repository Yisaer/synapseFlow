//! Local CLI for managing the encrypted secret store (VF-51 §6.1.3).
//!
//! `veloflux secrets <set|get|rm|list> ...`
//!
//! This is a debug/config-authoring tool: it opens the store offline with the
//! configured root key provider (env > hardcoded), mutates it, and re-encrypts.
//! There is NO management API — secrets never traverse the network.
//!
//! Secret VALUES are read from an interactive prompt, piped stdin, or
//! `--from-file PATH` — NEVER from a positional argv (which would leak into
//! shell history / `/proc/<pid>/cmdline` / CI logs). VF-51 §6.1.3.

use std::io::{self, IsTerminal, Read, Write};
use std::path::PathBuf;

use flow::secret::{default_root_key_provider, SecretStore, DEFAULT_STORE_FILE};

use crate::server::DEFAULT_DATA_DIR;

type CliResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

/// Entry point. `args` is the full process argv; `args[1]` is expected to be
/// `secrets`. Returns `Ok` after handling, so the caller exits without starting
/// the server.
pub fn run(args: &[String]) -> CliResult {
    let sub = args.get(2).map(String::as_str);
    let rest = args.get(3..).unwrap_or(&[]);
    match sub {
        Some("set") => cmd_set(rest),
        Some("get") => cmd_get(rest),
        Some("rm") | Some("remove") => cmd_rm(rest),
        Some("list") | Some("ls") => cmd_list(rest),
        Some(other) => {
            eprintln!("unknown secrets subcommand `{other}`");
            print_usage();
            Err("unknown subcommand".into())
        }
        None => {
            print_usage();
            Err("missing subcommand".into())
        }
    }
}

fn print_usage() {
    eprintln!(
        "usage: veloflux secrets <command> [--data-dir DIR]\n\
         \n\
         commands:\n\
         \x20 set NAME [--from-file PATH]   store a secret (value from prompt/stdin/file, never argv)\n\
         \x20 get NAME                      print a secret value (debug)\n\
         \x20 rm  NAME                      remove a secret\n\
         \x20 list                          list secret names"
    );
}

/// Parsed common options plus positional name (if any).
struct Opts {
    name: Option<String>,
    data_dir: String,
    from_file: Option<String>,
}

fn parse_opts(rest: &[String]) -> Result<Opts, Box<dyn std::error::Error + Send + Sync>> {
    let mut name = None;
    let mut data_dir = None;
    let mut from_file = None;
    let mut it = rest.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--data-dir" => {
                data_dir = it.next().cloned();
                if data_dir.is_none() {
                    return Err("--data-dir requires a value".into());
                }
            }
            "--from-file" => {
                from_file = it.next().cloned();
                if from_file.is_none() {
                    return Err("--from-file requires a value".into());
                }
            }
            other if other.starts_with("--") => {
                return Err(format!("unknown flag `{other}`").into());
            }
            other => {
                if name.is_some() {
                    return Err("unexpected extra positional argument".into());
                }
                name = Some(other.to_string());
            }
        }
    }
    Ok(Opts {
        name,
        data_dir: data_dir.unwrap_or_else(|| DEFAULT_DATA_DIR.to_string()),
        from_file,
    })
}

fn store_path(data_dir: &str) -> PathBuf {
    PathBuf::from(data_dir).join(DEFAULT_STORE_FILE)
}

fn require_name(opts: &Opts) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    opts.name
        .clone()
        .filter(|n| !n.trim().is_empty())
        .ok_or_else(|| "this command requires a secret NAME".into())
}

/// Read the secret value from `--from-file`, piped stdin, or an interactive
/// prompt. A single trailing newline is stripped. NEVER reads from argv.
fn read_secret_value(from_file: Option<&str>) -> io::Result<String> {
    let raw = match from_file {
        Some(path) => std::fs::read_to_string(path)?,
        None => {
            let mut stdin = io::stdin();
            if stdin.is_terminal() {
                eprint!("Enter secret value (input is not hidden): ");
                io::stderr().flush().ok();
                let mut line = String::new();
                stdin.read_line(&mut line)?;
                line
            } else {
                let mut buf = String::new();
                stdin.read_to_string(&mut buf)?;
                buf
            }
        }
    };
    Ok(strip_trailing_newline(raw))
}

fn strip_trailing_newline(mut s: String) -> String {
    if s.ends_with('\n') {
        s.pop();
        if s.ends_with('\r') {
            s.pop();
        }
    }
    s
}

fn cmd_set(rest: &[String]) -> CliResult {
    let opts = parse_opts(rest)?;
    let name = require_name(&opts)?;
    let value = read_secret_value(opts.from_file.as_deref())?;
    if value.is_empty() {
        return Err("refusing to store an empty secret value".into());
    }
    let provider = default_root_key_provider()?;
    let path = store_path(&opts.data_dir);
    let mut store = SecretStore::load(&path, provider.as_ref())?;
    let existed = store.contains(&name);
    store.set(&name, value);
    store.save(&path, provider.as_ref())?;
    eprintln!(
        "secret `{name}` {} ({})",
        if existed { "updated" } else { "created" },
        path.display()
    );
    Ok(())
}

fn cmd_get(rest: &[String]) -> CliResult {
    let opts = parse_opts(rest)?;
    let name = require_name(&opts)?;
    let provider = default_root_key_provider()?;
    let store = SecretStore::load(&store_path(&opts.data_dir), provider.as_ref())?;
    let value = store.get(&name)?;
    // Print the raw value to stdout (debug tool). No trailing newline mangling.
    print!("{}", &*value);
    io::stdout().flush().ok();
    Ok(())
}

fn cmd_rm(rest: &[String]) -> CliResult {
    let opts = parse_opts(rest)?;
    let name = require_name(&opts)?;
    let provider = default_root_key_provider()?;
    let path = store_path(&opts.data_dir);
    let mut store = SecretStore::load(&path, provider.as_ref())?;
    if store.remove(&name) {
        store.save(&path, provider.as_ref())?;
        eprintln!("secret `{name}` removed");
    } else {
        return Err(format!("secret `{name}` not found").into());
    }
    Ok(())
}

fn cmd_list(rest: &[String]) -> CliResult {
    let opts = parse_opts(rest)?;
    let provider = default_root_key_provider()?;
    let store = SecretStore::load(&store_path(&opts.data_dir), provider.as_ref())?;
    for name in store.names() {
        println!("{name}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_single_trailing_newline() {
        assert_eq!(strip_trailing_newline("abc\n".to_string()), "abc");
        assert_eq!(strip_trailing_newline("abc\r\n".to_string()), "abc");
        assert_eq!(strip_trailing_newline("a\nb".to_string()), "a\nb");
        assert_eq!(strip_trailing_newline("abc".to_string()), "abc");
    }

    #[test]
    fn parse_opts_reads_flags_and_name() {
        let args = vec![
            "mqtt-pass".to_string(),
            "--data-dir".to_string(),
            "/data".to_string(),
            "--from-file".to_string(),
            "/tmp/v".to_string(),
        ];
        let opts = parse_opts(&args).unwrap();
        assert_eq!(opts.name.as_deref(), Some("mqtt-pass"));
        assert_eq!(opts.data_dir, "/data");
        assert_eq!(opts.from_file.as_deref(), Some("/tmp/v"));
    }

    #[test]
    fn parse_opts_defaults_data_dir() {
        let opts = parse_opts(&["k".to_string()]).unwrap();
        assert_eq!(opts.data_dir, DEFAULT_DATA_DIR);
    }

    #[test]
    fn set_get_rm_list_roundtrip_via_file() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();
        let value_file = dir.path().join("val");
        std::fs::write(&value_file, "s3cr3t\n").unwrap();

        // set via --from-file (no argv value)
        cmd_set(&[
            "k1".to_string(),
            "--data-dir".to_string(),
            data_dir.clone(),
            "--from-file".to_string(),
            value_file.to_str().unwrap().to_string(),
        ])
        .unwrap();

        // get
        let provider = default_root_key_provider().unwrap();
        let store = SecretStore::load(&store_path(&data_dir), provider.as_ref()).unwrap();
        assert_eq!(&*store.get("k1").unwrap(), "s3cr3t");

        // list
        cmd_list(&["--data-dir".to_string(), data_dir.clone()]).unwrap();

        // rm
        cmd_rm(&["k1".to_string(), "--data-dir".to_string(), data_dir.clone()]).unwrap();
        let store = SecretStore::load(&store_path(&data_dir), provider.as_ref()).unwrap();
        assert!(!store.contains("k1"));
    }
}
