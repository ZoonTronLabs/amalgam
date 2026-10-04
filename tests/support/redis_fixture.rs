// CI makes Redis evidence mandatory with AMALGAM_REQUIRE_REDIS=1.

pub fn redis_url() -> Option<String> {
    let required = std::env::var("AMALGAM_REQUIRE_REDIS")
        .is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("true"));
    let url = match std::env::var("AMALGAM_REDIS_URL") {
        Ok(url) => url,
        Err(error) if required => panic!(
            "live Redis acceptance requires AMALGAM_REDIS_URL when AMALGAM_REQUIRE_REDIS=1: {error}"
        ),
        Err(std::env::VarError::NotPresent) => return None,
        Err(error) => panic!("invalid AMALGAM_REDIS_URL: {error}"),
    };
    assert!(
        !url.trim().is_empty(),
        "AMALGAM_REDIS_URL must not be blank"
    );
    redis::Client::open(url.as_str())
        .unwrap_or_else(|error| panic!("malformed AMALGAM_REDIS_URL: {error}"));
    Some(url)
}
