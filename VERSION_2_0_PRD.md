# HcDataCleanUpAi Version 2.0 - PRD
_Updated: February 13, 2026_

## 1) Vision
Deliver a production-oriented V2 that is faster to operate, easier to diagnose, and safer at scale for large healthcare datasets.

## 2) Success Criteria
- Async job flow supports deterministic cleanup lifecycle actions, including cancellation.
- Chunked uploads remain resumable, inspectable, and can be cancelled cleanly.
- Frontend becomes operator-ready with clearer status, progress, copy/share, and power-user controls.
- API endpoint targeting is configurable from UI without code changes.

## 3) Scope
- Keep the existing API contract (`/api/datasets`, `/api/uploads/*`, `/api/clean-jobs/*`) backward compatible.
- Expand runtime resilience for upload/job edges.
- Improve frontend ergonomics with high-impact UX upgrades.
- No new external dependencies.

## 4) Backend Improvements (Implemented in V2)
1. Upload session validation and metadata tracking:
   - Added upload metadata helpers and robust checks for extension, filename, chunk count, and file size before storing.
2. Upload chunk integrity and consistency:
   - Added index bounds/consistency checks and partial-chunk idempotent acceptance support.
3. New upload session endpoints:
   - `GET /uploads/{upload_id}` for session status.
   - `DELETE /uploads/{upload_id}` for cleanup/cancel.
4. Upload compatibility route mapping:
   - Proxy-compatible endpoints in `/compat` for delete/start/get and clean-job cancel.
5. Upload start/finalize hardening:
   - Added metadata payload validation and rejects malformed upload starts.
6. Async clean job cancellation endpoint:
   - `POST /clean-jobs/{job_id}/cancel`.
7. Async clean-cancel execution path:
   - Cancellation is tracked and surfaced to polling clients.
8. Run cancellation persistence:
   - DB job/run state updates when a clean job is canceled.
9. Run store improvements:
   - Added index for run status queries and bounded retention support for cleanup.
10. Direct upload filename/type guard:
    - Reject unsupported extensions before writing files on the backend.
11. Safety in feature wave parsing:
    - Robust wave filter to avoid conversion crashes.
12. API metadata/CORS hygiene:
    - Versioned API metadata and explicit CORS behavior based on allowed origins.

## 5) Frontend Improvements (Implemented in V2)
1. Runtime API base override:
   - Added UI controls to set/reset API base URL at runtime with localStorage persistence.
2. Upload drag-and-drop input:
   - Drag/drop zone with filename preview and immediate file attachment.
3. Upload session status + progress telemetry:
   - Chunk progress panel now shows bytes/second, ETA, and elapsed transfer metrics.
4. Upload cancellation UX:
   - Cancel button available and wired for both direct and chunked uploads.
5. Upload session visibility:
   - Current upload session + chunk status is displayed while chunked upload is active.
6. Clean-job cancellation UX:
   - Cancel button with polling feedback for async cleanup jobs.
7. Cleaner completion state handling:
   - Job result polling now handles canceled/failed states explicitly.
8. Keyboard shortcuts:
   - File focus, run, refresh, and fullscreen/escape shortcuts added.
9. Download link copy:
   - One-click copy of cleaned dataset URL.
10. QC JSON actions:
   - Copy-QC JSON and download-QC JSON actions.
11. Upload/session UI resets:
   - Cleanup of upload/cancel/metrics state after success, error, and cancel.
12. Google file import compatibility retained:
   - Existing import flow still works with improved status updates.
13. Safer client-side type validation:
   - Unsupported extension guard before upload begins.
14. Cleaner download action control:
   - Download actions enabled only after a successful cleaning run.
15. Better API status messaging:
   - Health status reflects selected API base during checks.

## 6) 15-Bullet Consolidated Change Set
1. API base configuration persisted in UI + localStorage.
2. Dynamic API URL resolution used by all frontend API calls.
3. Direct upload filename/type validation on client.
4. Upload dropzone dragover/leave/drop behavior.
5. Upload filename preview rendering.
6. Upload speed/ETA metrics in the upload status row.
7. Chunk upload session polling (`/uploads/{id}`).
8. Upload cancellation endpoint integration.
9. Upload cancel control and cleanup state.
10. Sync/async upload completion error handling with fallback path preserved.
11. Async clean job cancellation endpoint integration.
12. Clean run cancellation button + endpoint call.
13. Polling awareness for canceled jobs.
14. QC JSON copy and QC JSON download actions.
15. Copy button for cleaned download URL.

## 7) 10 High-Impact UI/UX Upgrades (Shipping)
1. One-screen API switching for local/proxy setups.
2. Drag-and-drop upload path with immediate filename confirmation.
3. Rich transfer telemetry (percent, bytes, throughput, ETA, elapsed).
4. Upload session panel to reduce uncertainty during large-file uploads.
5. Visible cancellation paths for long-running upload and clean jobs.
6. Keyboard-first operation shortcuts for power users.
7. Download URL copy action for quick downstream handoff.
8. Single-click QC JSON extraction (copy + file download).
9. Better run-state buttons behavior while operations are active.
10. Faster operator feedback at each stage via dynamic helper text and warnings.

## 8) Non-goals for this release
- New connectors beyond current placeholders.
- Full OAuth/Google flow redesign.
- New backend data warehouse integrations.
- Complex backend queue infrastructure.

## 9) Metrics to Track
- Cancellation success rate by operation type.
- Upload retry/recover percentage after failed chunk validation.
- Time-to-clean for files over 50MB.
- QC/export action adoption rate.
- API base override adoption in non-default deployments.

## 10) Rollout Checklist
- [ ] Validate large-chunk upload with direct cancellation.
- [ ] Validate async clean cancellation and final run state.
- [ ] Confirm QC JSON actions produce valid JSON payload.
- [ ] Confirm API base override works against alternate backend port.
- [ ] Validate drag/drop upload across desktop browsers.

