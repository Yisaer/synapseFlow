pub fn build_id() -> String {
    let sha = option_env!("BUILD_GIT_SHA").unwrap_or("unknown");
    let tag = option_env!("BUILD_GIT_TAG").unwrap_or("unknown");
    format!("{sha} {tag}")
}

