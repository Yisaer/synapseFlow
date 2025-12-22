use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/refs");

    let git_sha = run_git(&["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".to_string());
    let git_tag = run_git(&["describe", "--tags", "--always"]).unwrap_or_else(|| "unknown".to_string());

    println!("cargo:rustc-env=FLOW_GIT_SHA={}", git_sha);
    println!("cargo:rustc-env=FLOW_GIT_TAG={}", git_tag);
}

fn run_git(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if text.is_empty() {
        None
    } else {
        Some(text)
    }
}

