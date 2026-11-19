use std::{collections::HashSet, fmt, time::Duration};

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
#[cfg(any(debug_assertions, test))]
use reqwest::StatusCode;
use reqwest::{header, Client};
#[cfg(any(debug_assertions, test))]
use serde::Deserialize;
use serde_json::Value;
#[cfg(debug_assertions)]
use zcash_voting::pir::{
    classify_pir_snapshot_height, select_pir_endpoint, PirSnapshotEndpointDiagnostic,
    PirSnapshotEndpointStatus,
};
use zcash_voting::{
    config::{
        resolve_dynamic_voting_config, resolve_static_voting_config, PinnedConfigSource,
        ResolveVotingConfigOptions, ResolvedVotingConfig, VotingConfigError,
    },
    validate_proposal_id, validate_round_params, validate_vote_options, HyperTransport, RouteError,
    RouteFuture, RouteHttp, RouteRequest, RouteResponse, MAX_PROPOSAL_ID,
};

use crate::model::{AppOption, AppProposal, Profile, RoundSnapshot, ServiceEndpointView};

const MAINNET_CONFIG_SOURCE: &str = "https://voting.valargroup.dev/pins/prod/fb62a56fae28debfdaa092f163cda0dab13295f87d25bbc4d0064d6ccdeb6943/static-voting-config.json?checksum=sha256:fb62a56fae28debfdaa092f163cda0dab13295f87d25bbc4d0064d6ccdeb6943";
const TESTNET_CONFIG_SOURCE: &str = "https://voting.valargroup.dev/pins/stage/046758f8d1f1a74c7ea63461fd77101930c5df5817b74453ed81895c26bf988f/static-voting-config.json?checksum=sha256:046758f8d1f1a74c7ea63461fd77101930c5df5817b74453ed81895c26bf988f";
const MAX_CONFIG_BYTES: usize = 8 * 1024 * 1024;
const MAX_CHAIN_RESPONSE_BYTES: usize = 32 * 1024 * 1024;
#[cfg(debug_assertions)]
const MAX_PIR_ROOT_RESPONSE_BYTES: usize = 64 * 1024;
const MAX_RECENT_INACTIVE_ROUNDS: usize = 12;
const MAX_ROUNDS_PER_RESPONSE: usize = 4_096;
const MAX_TITLE_BYTES: usize = 512;
const MAX_DESCRIPTION_BYTES: usize = 64 * 1_024;
const MAX_OPTION_DESCRIPTION_BYTES: usize = 16 * 1_024;

/// Bound on connection setup, including TLS and any proxy tunnel.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(12);

pub fn http_client() -> Result<Client, String> {
    Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(Duration::from_secs(30))
        .user_agent(format!("valar-custody-voter/{}", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(|error| format!("build HTTP client failed: {error}"))
}

/// The SDK transport for vote-chain, vote-tree, and helper traffic.
pub type VotingTransport = HyperTransport<ReqwestRoute>;

/// Builds the SDK transport over [`ReqwestRoute`].
pub fn voting_transport() -> Result<VotingTransport, String> {
    // The SDK bounds each whole request, up to minutes for a chain POST. A
    // server that stalls during connection setup must fail over well before
    // that, and as a connect failure, which says no vote bytes left.
    let client = Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|error| format!("build voting HTTP client failed: {error}"))?;
    Ok(HyperTransport::with_route(ReqwestRoute { client }))
}

/// Executes the SDK's HTTP requests with reqwest, so voting traffic takes the
/// same proxy route as round discovery instead of connecting directly.
///
/// It follows no redirects, as [`RouteHttp`] requires, and sends no
/// User-Agent, like the SDK's own direct route, so custody traffic does not
/// stand out to helpers.
pub struct ReqwestRoute {
    client: Client,
}

impl RouteHttp for ReqwestRoute {
    fn execute<'a>(
        &'a self,
        request: RouteRequest<'a>,
        on_dispatch: &'a (dyn Fn() + Send + Sync),
    ) -> RouteFuture<'a> {
        Box::pin(async move {
            let mut builder = self
                .client
                .request(request.method, request.url)
                .timeout(request.timeout)
                .body(request.body);
            for (name, value) in request.headers {
                builder = builder.header(name.as_str(), value.as_str());
            }
            // reqwest has no hook between connection setup and the first
            // write, so dispatch is marked before both. A connect failure,
            // which reqwest reports distinctly, stays pre-dispatch.
            on_dispatch();
            let mut response = builder.send().await.map_err(|error| {
                let message = format!("send HTTP request: {error}");
                if error.is_connect() {
                    RouteError::before_dispatch(message)
                } else {
                    RouteError::after_dispatch(message)
                }
            })?;
            let status = response.status().as_u16();
            let headers = response
                .headers()
                .iter()
                .filter_map(|(name, value)| {
                    value
                        .to_str()
                        .ok()
                        .map(|value| (name.as_str().to_string(), value.to_string()))
                })
                .collect();
            let mut body = Vec::new();
            while let Some(chunk) = response.chunk().await.map_err(|error| {
                RouteError::response_read(format!("read HTTP response body: {error}"))
            })? {
                if body.len().saturating_add(chunk.len()) > request.max_response_bytes {
                    return Err(RouteError::response_read(format!(
                        "HTTP response exceeds {} bytes",
                        request.max_response_bytes
                    )));
                }
                body.extend_from_slice(&chunk);
            }
            Ok(RouteResponse {
                status,
                headers,
                body,
            })
        })
    }

    fn hook_precedes_connection_setup(&self) -> bool {
        true
    }
}

/// Why the pinned voting config could not be used.
#[derive(Debug)]
pub enum ConfigFailure {
    /// The config advertises a protocol version this build does not support.
    Unsupported(String),
    /// The config could not be fetched, parsed, or authenticated.
    Unavailable(String),
}

impl fmt::Display for ConfigFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (Self::Unsupported(message) | Self::Unavailable(message)) = self;
        formatter.write_str(message)
    }
}

/// Labels an SDK config error with the step that failed, keeping an
/// unsupported version apart from every other failure.
fn config_failure(step: &str, error: VotingConfigError) -> ConfigFailure {
    let message = format!("{step} failed: {error}");
    match error {
        VotingConfigError::UnsupportedVersion { .. } => ConfigFailure::Unsupported(message),
        _ => ConfigFailure::Unavailable(message),
    }
}

/// Fetches and authenticates the profile's pinned voting config.
pub async fn resolve_environment(
    client: &Client,
    profile: Profile,
) -> Result<ResolvedVotingConfig, ConfigFailure> {
    let source = match profile {
        Profile::Mainnet => MAINNET_CONFIG_SOURCE,
        Profile::Testnet => TESTNET_CONFIG_SOURCE,
        Profile::Demo => {
            return Err(ConfigFailure::Unavailable(
                "the local demo does not use remote voting config".to_string(),
            ))
        }
    };
    let parsed = PinnedConfigSource::parse(source)
        .map_err(|error| config_failure("parse pinned voting config source", error))?;
    let static_bytes = fetch_bytes(client, &parsed.url, MAX_CONFIG_BYTES)
        .await
        .map_err(ConfigFailure::Unavailable)?;
    let resolved_static = resolve_static_voting_config(source, &static_bytes)
        .map_err(|error| config_failure("authenticate static voting config", error))?;
    let dynamic_bytes = fetch_bytes(
        client,
        &resolved_static.dynamic_config_url,
        MAX_CONFIG_BYTES,
    )
    .await
    .map_err(ConfigFailure::Unavailable)?;
    resolve_dynamic_voting_config(
        resolved_static,
        &dynamic_bytes,
        ResolveVotingConfigOptions::default(),
    )
    .map_err(|error| config_failure("authenticate dynamic voting config", error))
}

pub async fn list_rounds(client: &Client, profile: Profile) -> Result<Vec<RoundSnapshot>, String> {
    if profile.is_demo() {
        return Ok(vec![crate::demo::round_snapshot()]);
    }
    let environment = resolve_environment(client, profile)
        .await
        .map_err(|failure| failure.to_string())?;
    let servers = vote_server_views(&environment);
    get_from_vote_servers(client, &servers, "/shielded-vote/v1/rounds", |response| {
        parse_round_list_response(profile, &environment, response)
    })
    .await
    .map(|(rounds, _)| rounds)
}

fn parse_round_list_response(
    profile: Profile,
    environment: &ResolvedVotingConfig,
    response: &[u8],
) -> Result<Vec<RoundSnapshot>, String> {
    let root: Value = serde_json::from_slice(response)
        .map_err(|error| format!("decode vote-chain round list failed: {error}"))?;
    let values = root
        .get("rounds")
        .and_then(Value::as_array)
        .ok_or_else(|| "vote-chain round list is missing rounds".to_string())?;
    if values.len() > MAX_ROUNDS_PER_RESPONSE {
        return Err(format!(
            "vote-chain returned more than {MAX_ROUNDS_PER_RESPONSE} rounds"
        ));
    }
    let authenticated: HashSet<&str> = environment
        .authenticated_rounds
        .iter()
        .map(|round| round.round_id.as_str())
        .collect();
    let mut rounds = Vec::new();
    let mut seen_rounds = HashSet::new();
    for value in values {
        let Some(round_id) = value
            .get("vote_round_id")
            .and_then(Value::as_str)
            .and_then(|encoded| decode_round_id(encoded).ok())
        else {
            continue;
        };
        if !authenticated.contains(round_id.as_str()) {
            continue;
        }
        if !seen_rounds.insert(round_id.clone()) {
            return Err(format!(
                "vote-chain returned authenticated round {round_id} more than once"
            ));
        }
        rounds.push(
            parse_round(profile, environment, value)
                .map_err(|error| format!("authenticated round {round_id} is invalid: {error}"))?,
        );
    }
    rounds.sort_by(|left, right| {
        right
            .is_active
            .cmp(&left.is_active)
            .then_with(|| right.vote_end_time.cmp(&left.vote_end_time))
            .then_with(|| right.snapshot_height.cmp(&left.snapshot_height))
    });
    let mut inactive_seen = 0usize;
    rounds.retain(|round| {
        if round.is_active {
            true
        } else if inactive_seen < MAX_RECENT_INACTIVE_ROUNDS {
            inactive_seen += 1;
            true
        } else {
            false
        }
    });
    Ok(rounds)
}

pub async fn fetch_round(
    client: &Client,
    profile: Profile,
    round_id: &str,
) -> Result<RoundSnapshot, String> {
    if profile.is_demo() {
        let round = crate::demo::round_snapshot();
        return if round.round_id == round_id {
            Ok(round)
        } else {
            Err("unknown local demo round".to_string())
        };
    }
    zcash_voting::types::validate_vote_round_id_hex(round_id)
        .map_err(|error| format!("invalid round id: {error}"))?;
    let environment = resolve_environment(client, profile)
        .await
        .map_err(|failure| failure.to_string())?;
    if !environment
        .authenticated_rounds
        .iter()
        .any(|round| round.round_id == round_id)
    {
        return Err(format!(
            "round {round_id} is not authenticated by the pinned voting config"
        ));
    }
    let path = format!("/shielded-vote/v1/round/{round_id}");
    let servers = vote_server_views(&environment);
    let (mut round, answered) = get_from_vote_servers(client, &servers, &path, |response| {
        parse_round_response(profile, &environment, round_id, response)
    })
    .await?;
    // The SDK sends a vote to only the first few configured servers, so the
    // one that just answered goes first.
    round.vote_servers.rotate_left(answered);
    Ok(round)
}

fn parse_round_response(
    profile: Profile,
    environment: &ResolvedVotingConfig,
    round_id: &str,
    response: &[u8],
) -> Result<RoundSnapshot, String> {
    let root: Value = serde_json::from_slice(response)
        .map_err(|error| format!("decode vote-chain round failed: {error}"))?;
    let value = root
        .get("round")
        .ok_or_else(|| "vote-chain round response is missing round".to_string())?;
    let round = parse_round(profile, environment, value)?;
    if round.round_id != round_id {
        return Err("vote-chain returned a different round id".to_string());
    }
    Ok(round)
}

fn parse_round(
    profile: Profile,
    environment: &ResolvedVotingConfig,
    value: &Value,
) -> Result<RoundSnapshot, String> {
    let round_id = decode_round_id(required_string(value, "vote_round_id")?)?;
    let snapshot_height = required_u64(value, "snapshot_height")?;
    let nc_root = decode_fixed_32(required_string(value, "nc_root")?, "nc_root")?;
    let nullifier_imt_root = decode_fixed_32(
        required_string(value, "nullifier_imt_root")?,
        "nullifier_imt_root",
    )?;
    let params = environment
        .trusted_voting_round_params(
            round_id.clone(),
            snapshot_height,
            nc_root,
            nullifier_imt_root,
        )
        .map_err(|error| format!("build trusted round parameters failed: {error}"))?;
    validate_round_params(&params)
        .map_err(|error| format!("vote-chain round parameters are invalid: {error}"))?;
    let proposals = parse_proposals(value.get("proposals"))?;
    let status = parse_status(value.get("status"))?;
    let (status_label, is_active) = status_details(&status);
    let vote_servers = vote_server_views(environment);
    Ok(RoundSnapshot {
        profile,
        chain_id: profile.chain_id().to_string(),
        round_id,
        title: optional_string(value, "title", MAX_TITLE_BYTES)?
            .unwrap_or_else(|| "Untitled vote".to_string()),
        description: optional_string(value, "description", MAX_DESCRIPTION_BYTES)?
            .unwrap_or_default(),
        status,
        status_label,
        is_active,
        snapshot_height,
        vote_end_time: optional_u64(value, "vote_end_time")?.unwrap_or(0),
        ceremony_start_time: optional_u64(value, "ceremony_phase_start")?,
        proposals,
        params,
        vote_servers,
        authenticated: true,
    })
}

fn vote_server_views(environment: &ResolvedVotingConfig) -> Vec<ServiceEndpointView> {
    environment
        .vote_servers
        .iter()
        .map(|endpoint| ServiceEndpointView {
            url: endpoint.url.clone(),
            label: endpoint.label.clone(),
        })
        .collect()
}

fn parse_proposals(value: Option<&Value>) -> Result<Vec<AppProposal>, String> {
    let proposals = value
        .and_then(Value::as_array)
        .ok_or_else(|| "vote-chain round is missing proposals".to_string())?;
    if proposals.is_empty() {
        return Err("vote-chain round has no proposals".to_string());
    }
    if proposals.len() > MAX_PROPOSAL_ID as usize {
        return Err(format!(
            "vote-chain round has more than {MAX_PROPOSAL_ID} proposals"
        ));
    }
    let mut seen_proposals = HashSet::new();
    proposals
        .iter()
        .map(|proposal| {
            let id = required_u64(proposal, "id")?;
            let id = u32::try_from(id).map_err(|_| "proposal id exceeds u32".to_string())?;
            validate_proposal_id(id)
                .map_err(|error| format!("vote-chain proposal is invalid: {error}"))?;
            if !seen_proposals.insert(id) {
                return Err(format!(
                    "vote-chain proposal id {id} appears more than once"
                ));
            }
            let options = proposal
                .get("options")
                .and_then(Value::as_array)
                .ok_or_else(|| format!("proposal {id} is missing options"))?;
            validate_vote_options(
                u32::try_from(options.len())
                    .map_err(|_| format!("proposal {id} has too many options"))?,
            )
            .map_err(|error| format!("vote-chain proposal {id} is invalid: {error}"))?;
            let options = options
                .iter()
                .enumerate()
                .map(|(position, option)| {
                    let expected = u32::try_from(position)
                        .map_err(|_| "option index exceeds u32".to_string())?;
                    let index = optional_u64(option, "index")?
                        .map(|index| {
                            u32::try_from(index)
                                .map_err(|_| format!("proposal {id} option index exceeds u32"))
                        })
                        .transpose()?
                        .unwrap_or(expected);
                    if index != expected {
                        return Err(format!(
                            "proposal {id} option indices must be contiguous from zero"
                        ));
                    }
                    Ok(AppOption {
                        index,
                        label: optional_string(option, "label", MAX_TITLE_BYTES)?
                            .unwrap_or_else(|| format!("Option {}", index + 1)),
                        description: optional_string(
                            option,
                            "description",
                            MAX_OPTION_DESCRIPTION_BYTES,
                        )?
                        .unwrap_or_default(),
                    })
                })
                .collect::<Result<Vec<_>, String>>()?;
            Ok(AppProposal {
                id,
                title: optional_string(proposal, "title", MAX_TITLE_BYTES)?
                    .unwrap_or_else(|| format!("Proposal {id}")),
                description: optional_string(proposal, "description", MAX_DESCRIPTION_BYTES)?
                    .unwrap_or_default(),
                options,
            })
        })
        .collect()
}

#[cfg(debug_assertions)]
pub async fn submit_delegation(
    client: &Client,
    servers: &[ServiceEndpointView],
    body: &str,
) -> Result<BroadcastResult, String> {
    post_vote_servers(client, servers, "/shielded-vote/v1/delegate-vote", body).await
}

#[cfg(debug_assertions)]
pub async fn resolve_pir_endpoint(
    client: &Client,
    environment: &ResolvedVotingConfig,
    snapshot_height: u64,
) -> Result<String, String> {
    let mut diagnostics = Vec::with_capacity(environment.pir_endpoints.len());
    for endpoint in &environment.pir_endpoints {
        let root_url = endpoint_url(&endpoint.url, "/root");
        let diagnostic = match client.get(&root_url).send().await {
            Ok(response) => {
                let status = response.status();
                match bounded_response(response, MAX_PIR_ROOT_RESPONSE_BYTES).await {
                    Err(error) => PirSnapshotEndpointDiagnostic {
                        endpoint: endpoint.url.clone(),
                        status: PirSnapshotEndpointStatus::MalformedJson,
                        reported_height: None,
                        http_status_code: Some(status.as_u16()),
                        message: Some(error),
                    },
                    Ok(bytes) if !status.is_success() => PirSnapshotEndpointDiagnostic {
                        endpoint: endpoint.url.clone(),
                        status: PirSnapshotEndpointStatus::NonSuccessStatus,
                        reported_height: None,
                        http_status_code: Some(status.as_u16()),
                        message: Some(response_message(&bytes)),
                    },
                    Ok(bytes) => match serde_json::from_slice::<Value>(&bytes) {
                        Ok(root) => match parse_pir_root_height(&root) {
                            Ok(height) => classify_pir_snapshot_height(
                                endpoint.url.clone(),
                                snapshot_height,
                                height,
                            ),
                            Err(message) => PirSnapshotEndpointDiagnostic {
                                endpoint: endpoint.url.clone(),
                                status: PirSnapshotEndpointStatus::MalformedJson,
                                reported_height: None,
                                http_status_code: Some(status.as_u16()),
                                message: Some(message),
                            },
                        },
                        Err(error) => PirSnapshotEndpointDiagnostic {
                            endpoint: endpoint.url.clone(),
                            status: PirSnapshotEndpointStatus::MalformedJson,
                            reported_height: None,
                            http_status_code: Some(status.as_u16()),
                            message: Some(error.to_string()),
                        },
                    },
                }
            }
            Err(error) => PirSnapshotEndpointDiagnostic {
                endpoint: endpoint.url.clone(),
                status: PirSnapshotEndpointStatus::TimeoutOrNetworkError,
                reported_height: None,
                http_status_code: None,
                message: Some(error.to_string()),
            },
        };
        diagnostics.push(diagnostic);
    }

    select_pir_endpoint(&diagnostics, snapshot_height, 0)
        .map(|resolution| resolution.endpoint)
        .map_err(|error| {
            let summary = diagnostics
                .iter()
                .map(|diagnostic| match diagnostic.reported_height {
                    Some(height) => format!("{}={height}", diagnostic.endpoint),
                    None => format!("{}={:?}", diagnostic.endpoint, diagnostic.status),
                })
                .collect::<Vec<_>>()
                .join("; ");
            format!("resolve exact-height PIR endpoint failed: {error}. {summary}")
        })
}

/// Looks up one delegation transaction on the configured vote servers.
///
/// Returns `None` while every server reports it as not yet committed, and an
/// error only when every server failed. The test custodian uses this to avoid
/// rebroadcasting a delegation that already landed.
#[cfg(any(debug_assertions, test))]
pub async fn get_tx_confirmation(
    client: &Client,
    servers: &[ServiceEndpointView],
    tx_hash: &str,
) -> Result<Option<TxConfirmation>, String> {
    validate_tx_hash(tx_hash)?;
    let path = format!("/shielded-vote/v1/tx/{tx_hash}");
    let mut errors = Vec::new();
    for server in servers {
        let url = endpoint_url(&server.url, &path);
        match client.get(&url).send().await {
            Ok(response) if response.status() == StatusCode::NOT_FOUND => continue,
            Ok(response) => {
                let status = response.status();
                let bytes = match bounded_response(response, MAX_CHAIN_RESPONSE_BYTES).await {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        errors.push(format!("{}: {error}", server.label));
                        continue;
                    }
                };
                if status.is_success() || status == StatusCode::UNPROCESSABLE_ENTITY {
                    match serde_json::from_slice::<TxConfirmation>(&bytes) {
                        Ok(confirmation) => return Ok(Some(confirmation)),
                        Err(error) => errors.push(format!(
                            "{} returned a malformed transaction confirmation: {error}",
                            server.label
                        )),
                    }
                } else {
                    errors.push(format!("{} returned HTTP {status}", server.label));
                }
            }
            Err(error) => errors.push(format!("{}: {error}", server.label)),
        }
    }
    if errors.len() == servers.len() && !servers.is_empty() {
        return Err(format!(
            "all vote servers failed while checking transaction: {}",
            errors.join("; ")
        ));
    }
    Ok(None)
}

#[cfg(any(debug_assertions, test))]
#[derive(Clone, Debug, Deserialize)]
pub struct BroadcastResult {
    pub tx_hash: String,
    pub code: u64,
    #[serde(default)]
    pub log: String,
}

#[cfg(any(debug_assertions, test))]
#[derive(Clone, Debug, Deserialize)]
pub struct TxConfirmation {
    pub code: u64,
    #[serde(default)]
    pub log: String,
}

#[cfg(any(debug_assertions, test))]
async fn post_vote_servers(
    client: &Client,
    servers: &[ServiceEndpointView],
    path: &str,
    body: &str,
) -> Result<BroadcastResult, String> {
    let mut errors = Vec::new();
    for server in servers {
        let url = endpoint_url(&server.url, path);
        match client
            .post(&url)
            .header(header::CONTENT_TYPE, "application/json")
            .body(body.to_owned())
            .send()
            .await
        {
            Ok(response) => {
                let status = response.status();
                let bytes = match bounded_response(response, MAX_CHAIN_RESPONSE_BYTES).await {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        errors.push(format!("{}: {error}", server.label));
                        continue;
                    }
                };
                if status.is_success() || status == StatusCode::UNPROCESSABLE_ENTITY {
                    let result: BroadcastResult = match serde_json::from_slice(&bytes) {
                        Ok(result) => result,
                        Err(error) => {
                            errors.push(format!(
                                "{} returned a malformed vote submission response: {error}",
                                server.label
                            ));
                            continue;
                        }
                    };
                    if result.code != 0 {
                        return Err(if result.log.trim().is_empty() {
                            format!(
                                "vote chain rejected the transaction with code {}",
                                result.code
                            )
                        } else {
                            response_message(result.log.as_bytes())
                        });
                    }
                    if let Err(error) = validate_tx_hash(&result.tx_hash) {
                        errors.push(format!(
                            "{} returned an invalid transaction hash: {error}",
                            server.label
                        ));
                        continue;
                    }
                    return Ok(result);
                }
                if status.is_server_error() {
                    errors.push(format!("{} returned HTTP {status}", server.label));
                    continue;
                }
                return Err(format!(
                    "{} rejected the vote with HTTP {status}: {}",
                    server.label,
                    response_message(&bytes)
                ));
            }
            Err(error) => errors.push(format!("{}: {error}", server.label)),
        }
    }
    Err(format!("all vote servers failed: {}", errors.join("; ")))
}

/// Returns the first validated answer and the index of the server that gave it.
async fn get_from_vote_servers<T, F>(
    client: &Client,
    servers: &[ServiceEndpointView],
    path: &str,
    mut validate: F,
) -> Result<(T, usize), String>
where
    F: FnMut(&[u8]) -> Result<T, String>,
{
    let mut errors = Vec::new();
    for (index, server) in servers.iter().enumerate() {
        let url = endpoint_url(&server.url, path);
        match client
            .get(&url)
            .header(header::CACHE_CONTROL, "no-cache")
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => {
                let bytes = match bounded_response(response, MAX_CHAIN_RESPONSE_BYTES).await {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        errors.push(format!("{}: {error}", server.label));
                        continue;
                    }
                };
                match validate(&bytes) {
                    Ok(value) => return Ok((value, index)),
                    Err(error) => errors.push(format!("{}: {error}", server.label)),
                }
            }
            Ok(response) => errors.push(format!(
                "{} returned HTTP {}",
                server.label,
                response.status()
            )),
            Err(error) => errors.push(format!("{}: {error}", server.label)),
        }
    }
    Err(format!("all vote servers failed: {}", errors.join("; ")))
}

async fn fetch_bytes(client: &Client, url: &str, limit: usize) -> Result<Vec<u8>, String> {
    let response = client
        .get(url)
        .header(header::CACHE_CONTROL, "no-cache")
        .header(header::PRAGMA, "no-cache")
        .send()
        .await
        .map_err(|error| format!("fetch {url} failed: {error}"))?;
    if !response.status().is_success() {
        return Err(format!("fetch {url} returned HTTP {}", response.status()));
    }
    bounded_response(response, limit).await
}

async fn bounded_response(
    mut response: reqwest::Response,
    limit: usize,
) -> Result<Vec<u8>, String> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(format!("HTTP response exceeds {limit} bytes"));
    }
    let mut bytes = Vec::with_capacity(
        response
            .content_length()
            .and_then(|length| usize::try_from(length).ok())
            .unwrap_or(0)
            .min(limit),
    );
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| format!("read HTTP response failed: {error}"))?
    {
        if bytes.len().saturating_add(chunk.len()) > limit {
            return Err(format!("HTTP response exceeds {limit} bytes"));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn endpoint_url(base: &str, path: &str) -> String {
    format!("{}{}", base.trim_end_matches('/'), path)
}

#[cfg(any(debug_assertions, test))]
fn parse_pir_root_height(root: &Value) -> Result<Option<u64>, String> {
    let Some(value) = root.get("height") else {
        return Ok(None);
    };
    match value {
        Value::Number(number) => number
            .as_u64()
            .map(Some)
            .ok_or_else(|| "PIR root height must be an unsigned integer".to_string()),
        Value::String(value)
            if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) =>
        {
            value
                .parse::<u64>()
                .map(Some)
                .map_err(|_| "PIR root height is outside the u64 range".to_string())
        }
        _ => Err("PIR root height must be an unsigned integer or decimal string".to_string()),
    }
}

fn decode_round_id(encoded: &str) -> Result<String, String> {
    let bytes = BASE64_STANDARD
        .decode(encoded.as_bytes())
        .map_err(|error| format!("decode vote round id failed: {error}"))?;
    if bytes.len() != 32 {
        return Err(format!(
            "vote round id must decode to 32 bytes, got {}",
            bytes.len()
        ));
    }
    let round_id = hex::encode(bytes);
    zcash_voting::types::validate_vote_round_id_hex(&round_id)
        .map_err(|error| format!("vote round id is invalid: {error}"))?;
    Ok(round_id)
}

fn decode_fixed_32(encoded: &str, field: &str) -> Result<Vec<u8>, String> {
    let bytes = BASE64_STANDARD
        .decode(encoded.as_bytes())
        .map_err(|error| format!("decode {field} failed: {error}"))?;
    if bytes.len() != 32 {
        return Err(format!(
            "{field} must decode to 32 bytes, got {}",
            bytes.len()
        ));
    }
    Ok(bytes)
}

fn required_string<'a>(value: &'a Value, field: &str) -> Result<&'a str, String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("vote-chain round is missing {field}"))
}

fn optional_string(value: &Value, field: &str, max_bytes: usize) -> Result<Option<String>, String> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(raw)) if raw.len() <= max_bytes => Ok(Some(raw.clone())),
        Some(Value::String(_)) => Err(format!("{field} exceeds {max_bytes} bytes")),
        Some(_) => Err(format!("{field} must be a string")),
    }
}

fn required_u64(value: &Value, field: &str) -> Result<u64, String> {
    optional_u64(value, field)?.ok_or_else(|| format!("vote-chain round is missing {field}"))
}

fn optional_u64(value: &Value, field: &str) -> Result<Option<u64>, String> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(number)) => number
            .as_u64()
            .map(Some)
            .ok_or_else(|| format!("{field} must be an unsigned integer")),
        Some(Value::String(raw)) => raw
            .parse::<u64>()
            .map(Some)
            .map_err(|_| format!("{field} must be an unsigned integer")),
        Some(_) => Err(format!("{field} must be an unsigned integer")),
    }
}

fn parse_status(value: Option<&Value>) -> Result<String, String> {
    match value {
        None | Some(Value::Null) => Ok("unknown".to_string()),
        Some(Value::String(value)) if value.len() <= 64 => Ok(value.clone()),
        Some(Value::String(_)) => Err("round status exceeds 64 bytes".to_string()),
        Some(Value::Number(value)) => Ok(value.to_string()),
        Some(_) => Err("round status must be a string or integer".to_string()),
    }
}

#[cfg(any(debug_assertions, test))]
fn validate_tx_hash(tx_hash: &str) -> Result<(), String> {
    if tx_hash.len() == 64 && tx_hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err("vote-chain transaction hash must be exactly 64 hexadecimal characters".to_string())
    }
}

fn status_details(status: &str) -> (String, bool) {
    let normalized = status.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "1" | "active" | "session_status_active" => ("Active".to_string(), true),
        "2" | "tallying" | "session_status_tallying" => ("Tallying".to_string(), false),
        "3" | "finalized" | "session_status_finalized" => ("Finalized".to_string(), false),
        "4" | "pending" | "session_status_pending" => ("Pending".to_string(), false),
        "5" | "ceremony_failed" | "session_status_ceremony_failed" => {
            ("Ceremony failed".to_string(), false)
        }
        _ => ("Unknown".to_string(), false),
    }
}

#[cfg(any(debug_assertions, test))]
fn response_message(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).chars().take(500).collect()
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::TcpListener,
        thread,
    };

    use super::*;

    #[test]
    fn unsupported_config_versions_stay_apart_from_other_failures() {
        let unsupported = VotingConfigError::UnsupportedVersion {
            component: "vote_protocol".to_string(),
            advertised: "9".to_string(),
        };
        let undecodable = VotingConfigError::DecodeFailed {
            message: "truncated".to_string(),
        };

        assert!(matches!(
            config_failure("resolve", unsupported),
            ConfigFailure::Unsupported(_)
        ));
        assert!(matches!(
            config_failure("resolve", undecodable),
            ConfigFailure::Unavailable(_)
        ));
    }

    fn response_server(label: &str, body: &str) -> (ServiceEndpointView, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let body = body.to_string();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0u8; 4096];
            let _ = stream.read(&mut request).unwrap();
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
        });
        (
            ServiceEndpointView {
                url: format!("http://{address}"),
                label: label.to_string(),
            },
            handle,
        )
    }

    #[tokio::test]
    async fn voting_transport_returns_redirects_without_following_them() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        // The server answers once, so following the redirect would fail.
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0u8; 4096];
            let _ = stream.read(&mut request).unwrap();
            write!(
                stream,
                "HTTP/1.1 307 Temporary Redirect\r\nLocation: http://{address}/elsewhere\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            .unwrap();
        });
        let transport = voting_transport().unwrap();
        let response = zcash_voting::HelperTransport::get(
            &transport,
            &format!("http://{address}/shielded-vote/v1/status"),
            Duration::from_secs(5),
        )
        .await
        .unwrap();

        assert_eq!(response.status(), 307);
        handle.join().unwrap();
    }

    #[test]
    fn active_status_accepts_numeric_and_proto_names() {
        assert!(status_details("1").1);
        assert!(status_details("SESSION_STATUS_ACTIVE").1);
        assert!(!status_details("3").1);
    }

    #[test]
    fn voting_config_sources_use_resilient_immutable_pins() {
        for (source, environment, checksum) in [
            (
                MAINNET_CONFIG_SOURCE,
                "prod",
                "fb62a56fae28debfdaa092f163cda0dab13295f87d25bbc4d0064d6ccdeb6943",
            ),
            (
                TESTNET_CONFIG_SOURCE,
                "stage",
                "046758f8d1f1a74c7ea63461fd77101930c5df5817b74453ed81895c26bf988f",
            ),
        ] {
            let expected_url = format!(
                "https://voting.valargroup.dev/pins/{environment}/{checksum}/static-voting-config.json"
            );
            let parsed = PinnedConfigSource::parse(source).unwrap();
            assert_eq!(parsed.url, expected_url);
            assert_eq!(source, format!("{expected_url}?checksum=sha256:{checksum}"));
        }
    }

    #[test]
    fn base64_round_ids_are_normalized_to_lowercase_hex() {
        let encoded = BASE64_STANDARD.encode([1u8; 32]);
        assert_eq!(decode_round_id(&encoded).unwrap(), "01".repeat(32));
    }

    #[test]
    fn proposal_parser_rejects_duplicate_ids() {
        let proposals = serde_json::json!([
            {"id": 1, "options": [{"label": "Yes"}, {"label": "No"}]},
            {"id": 1, "options": [{"label": "A"}, {"label": "B"}]}
        ]);
        assert!(parse_proposals(Some(&proposals))
            .unwrap_err()
            .contains("appears more than once"));
    }

    #[test]
    fn proposal_parser_accepts_current_rounds_and_circuit_boundary() {
        for count in [37, 50] {
            let proposals = serde_json::json!((1..=count)
                .map(|id| serde_json::json!({
                    "id": id,
                    "options": [{"label": "Yes"}, {"index": 1, "label": "No"}]
                }))
                .collect::<Vec<_>>());
            let parsed = parse_proposals(Some(&proposals)).unwrap();
            assert_eq!(parsed.len(), count as usize);
            assert_eq!(parsed.last().unwrap().id, count);
        }
        for id in [0, 51] {
            let proposals = serde_json::json!([
                {"id": id, "options": [{"label": "Yes"}, {"index": 1, "label": "No"}]}
            ]);
            assert!(parse_proposals(Some(&proposals)).is_err());
        }
    }

    #[tokio::test]
    #[ignore = "reads authenticated rounds from live Mainnet and Testnet services"]
    async fn live_networks_load_authenticated_rounds() {
        let client = http_client().unwrap();
        for profile in [Profile::Testnet, Profile::Mainnet] {
            let environment = resolve_environment(&client, profile).await.unwrap();
            // Check each configured Valar replica independently so failover cannot
            // hide an incompatible response. Other operators may be unavailable.
            let servers = vote_server_views(&environment);
            for server in servers
                .iter()
                .filter(|server| server.url.contains(".valargroup.org"))
            {
                let bytes = fetch_bytes(
                    &client,
                    &endpoint_url(&server.url, "/shielded-vote/v1/rounds"),
                    MAX_CHAIN_RESPONSE_BYTES,
                )
                .await
                .unwrap();
                let rounds = parse_round_list_response(profile, &environment, &bytes).unwrap();
                assert!(
                    !rounds.is_empty(),
                    "{} returned no authenticated rounds",
                    server.url
                );
                assert!(rounds.iter().all(|round| round.authenticated));
                println!(
                    "{}: {} authenticated rounds, largest roster {}",
                    server.url,
                    rounds.len(),
                    rounds
                        .iter()
                        .map(|round| round.proposals.len())
                        .max()
                        .unwrap()
                );
            }
            let rounds = list_rounds(&client, profile).await.unwrap();
            assert!(!rounds.is_empty());
            let round = fetch_round(&client, profile, &rounds[0].round_id)
                .await
                .unwrap();
            assert_eq!(round.params, rounds[0].params);
            assert_eq!(round.proposals, rounds[0].proposals);
        }
    }

    #[test]
    fn transaction_hashes_are_path_safe_hex() {
        assert!(validate_tx_hash(&"AB".repeat(32)).is_ok());
        assert!(validate_tx_hash("../../unexpected").is_err());
        assert!(validate_tx_hash(&"a".repeat(63)).is_err());
    }

    #[tokio::test]
    async fn confirmation_check_skips_a_malformed_server() {
        let (malformed, malformed_handle) = response_server("malformed", "{");
        let (healthy, healthy_handle) =
            response_server("healthy", r#"{"code":0,"log":"confirmed","events":[]}"#);
        let confirmation = get_tx_confirmation(
            &http_client().unwrap(),
            &[malformed, healthy],
            &"ab".repeat(32),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(confirmation.code, 0);
        assert_eq!(confirmation.log, "confirmed");
        malformed_handle.join().unwrap();
        healthy_handle.join().unwrap();
    }

    #[tokio::test]
    async fn validated_get_skips_a_malformed_success_response() {
        let (malformed, malformed_handle) = response_server("malformed", "{");
        let (healthy, healthy_handle) = response_server("healthy", r#"{"rounds":[]}"#);
        let (response, answered) = get_from_vote_servers(
            &http_client().unwrap(),
            &[malformed, healthy],
            "/shielded-vote/v1/rounds",
            |bytes| {
                serde_json::from_slice::<Value>(bytes)
                    .map_err(|error| format!("decode round list failed: {error}"))
            },
        )
        .await
        .unwrap();

        assert_eq!(response, serde_json::json!({"rounds": []}));
        assert_eq!(answered, 1);
        malformed_handle.join().unwrap();
        healthy_handle.join().unwrap();
    }

    #[tokio::test]
    async fn broadcast_skips_a_malformed_success_response() {
        let (malformed, malformed_handle) = response_server("malformed", "{");
        let tx_hash = "cd".repeat(32);
        let healthy_body = format!(r#"{{"tx_hash":"{tx_hash}","code":0,"log":""}}"#);
        let (healthy, healthy_handle) = response_server("healthy", &healthy_body);
        let result = post_vote_servers(
            &http_client().unwrap(),
            &[malformed, healthy],
            "/shielded-vote/v1/cast-vote",
            "{}",
        )
        .await
        .unwrap();

        assert_eq!(result.tx_hash, tx_hash);
        malformed_handle.join().unwrap();
        healthy_handle.join().unwrap();
    }

    #[test]
    fn pir_root_height_accepts_only_unsigned_integer_encodings() {
        assert_eq!(parse_pir_root_height(&serde_json::json!({})).unwrap(), None);
        assert_eq!(
            parse_pir_root_height(&serde_json::json!({"height": 42})).unwrap(),
            Some(42)
        );
        assert_eq!(
            parse_pir_root_height(&serde_json::json!({"height": "42"})).unwrap(),
            Some(42)
        );
        assert!(parse_pir_root_height(&serde_json::json!({"height": -1})).is_err());
        assert!(parse_pir_root_height(&serde_json::json!({"height": " 42"})).is_err());
    }
}
