pub fn flow_build_id() -> String {
    let sha = option_env!("FLOW_GIT_SHA").unwrap_or("unknown");
    let tag = option_env!("FLOW_GIT_TAG").unwrap_or("unknown");
    format!("{sha} {tag}")
}

