//! One cancellable helper-tracking run for the selected workspace.
use crate::{
    model::{Profile, RoundSnapshot},
    storage::{open_db, ProfilePaths},
    voter,
};
use std::sync::{Arc, RwLock};
use tauri::{AppHandle, Emitter};
use zcash_voting::{
    prelude::*,
    share_tracking_drive::{ShareTrackingHostSourceBridge, ShareTrackingReporterBridge},
};

pub struct TrackingTask {
    pub profile: Profile,
    pub round_id: String,
    context: Arc<RwLock<RoundSnapshot>>,
    control: ChainSubmissionControl,
    handle: tauri::async_runtime::JoinHandle<()>,
}
impl TrackingTask {
    pub fn update(&self, round: RoundSnapshot) -> Result<(), String> {
        *self
            .context
            .write()
            .map_err(|_| "tracking context lock poisoned")? = round;
        Ok(())
    }
    pub async fn stop(self) {
        self.control.cancel();
        let _ = self.handle.await;
    }
    pub fn start(
        app: AppHandle,
        paths: ProfilePaths,
        round: RoundSnapshot,
    ) -> Result<Self, String> {
        let db = Arc::new(open_db(&paths, round.profile)?);
        let profile = round.profile;
        let round_id = round.round_id.clone();
        let context = Arc::new(RwLock::new(round));
        let control = ChainSubmissionControl::new(0);
        let run_context = Arc::clone(&context);
        let run_control = control.clone();
        let run_id = round_id.clone();
        let handle = tauri::async_runtime::spawn(async move {
            let client = if profile.is_demo() {
                HelperClient::new(Arc::new(crate::demo::DemoHelper), HelperHealth::default())
            } else {
                voter::helper_client()
            };
            let host = ShareTrackingHostSourceBridge::new(|| {
                let round = run_context.read().unwrap_or_else(|e| e.into_inner());
                ShareTrackingHostContext {
                    configured_helper_urls: voter::server_urls(&round),
                    now_seconds: if profile.is_demo() {
                        round.vote_end_time - 1
                    } else {
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|v| v.as_secs())
                            .unwrap_or_else(|_| {
                                run_control.cancel();
                                0
                            })
                    },
                    vote_end_time_seconds: Some(round.vote_end_time),
                }
            });
            let reporter = ShareTrackingReporterBridge::new(|_| {
                let _ = app.emit("share-progress", &run_id);
            });
            let report = ShareTrackingDriver::new(&db, &client, &run_id)
                .run(&host, &run_control, &reporter)
                .await;
            if !report.failures.is_empty() || !report.unrecoverable.is_empty() {
                let _=app.emit("share-tracking-error",format!("Helper tracking needs attention for round {}. Reopen the workspace to retry.",run_id));
            }
            let _ = app.emit("share-progress", &run_id);
        });
        Ok(Self {
            profile,
            round_id,
            context,
            control,
            handle,
        })
    }
}
