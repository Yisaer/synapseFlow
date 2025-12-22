pub fn git_sha() -> &'static str {
    option_env!("BUILD_GIT_SHA").unwrap_or("unknown")
}

pub fn git_tag() -> &'static str {
    option_env!("BUILD_GIT_TAG").unwrap_or("unknown")
}

pub fn build_id() -> String {
    format!("{} {}", git_sha(), git_tag())
}
