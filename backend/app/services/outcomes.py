from __future__ import annotations

from typing import Any, Dict, List

CRITICAL_OUTCOME_IDS = {"critical_date_integrity", "pii_safety", "rag_readiness_score"}


def _status_for_threshold(
    value: float,
    *,
    pass_max: float | None = None,
    warn_max: float | None = None,
    pass_min: float | None = None,
    warn_min: float | None = None,
) -> str:
    if pass_max is not None:
        if value <= pass_max:
            return "pass"
        if warn_max is not None and value <= warn_max:
            return "warn"
        return "fail"

    if pass_min is not None:
        if value >= pass_min:
            return "pass"
        if warn_min is not None and value >= warn_min:
            return "warn"
        return "fail"

    return "warn"


def _avg(values: Dict[str, float] | None) -> float:
    if not values:
        return 0.0
    numeric = [float(value) for value in values.values()]
    if not numeric:
        return 0.0
    return sum(numeric) / len(numeric)


def _sum_int(values: Dict[str, int] | None) -> int:
    if not values:
        return 0
    return int(sum(int(value) for value in values.values()))


def _high_severity_count(issues: List[Dict[str, Any]] | None) -> int:
    if not issues:
        return 0
    return sum(1 for issue in issues if (issue.get("severity") or "").lower() == "high")


def _outcome(
    *,
    outcome_id: str,
    label: str,
    status: str,
    target: str,
    observed_value: float | int | str,
    unit: str,
    evidence: str,
    recommended_action: str,
) -> Dict[str, Any]:
    return {
        "id": outcome_id,
        "label": label,
        "status": status,
        "target": target,
        "observed_value": observed_value,
        "unit": unit,
        "evidence": evidence,
        "recommended_action": recommended_action,
    }


def _quality_gate_status(failed_outcomes: List[str]) -> str:
    if not failed_outcomes:
        return "pass"
    if any(outcome_id in CRITICAL_OUTCOME_IDS for outcome_id in failed_outcomes):
        return "fail"
    return "warn"


def build_postclean_decision(*, qc_report: Dict[str, Any]) -> Dict[str, Any]:
    outcomes = qc_report.get("outcomes") or []
    quality_gate = qc_report.get("quality_gate") or {}

    failed = [item for item in outcomes if str(item.get("status") or "").lower() == "fail"]
    warned = [item for item in outcomes if str(item.get("status") or "").lower() == "warn"]
    failed_ids = [str(item.get("id")) for item in failed if item.get("id")]

    status = str(quality_gate.get("status") or "").lower()
    if status not in {"pass", "warn", "fail"}:
        status = _quality_gate_status(failed_ids)

    blockers: List[str] = []
    for item in failed:
        outcome_id = str(item.get("id") or "")
        if outcome_id in CRITICAL_OUTCOME_IDS:
            blockers.append(f"{item.get('label') or outcome_id}: {item.get('evidence') or 'failed'}")

    actions: List[str] = []
    for item in failed + warned:
        action = str(item.get("recommended_action") or "").strip()
        if action and action not in actions:
            actions.append(action)

    if status == "fail":
        release_recommendation = "Do not release; resolve critical blockers first."
    elif status == "warn":
        release_recommendation = "Release with caution after addressing warning outcomes."
    else:
        release_recommendation = "Release recommended."

    if not actions:
        actions.append("No immediate remediation actions required.")

    return {
        "status": status,
        "release_recommendation": release_recommendation,
        "blockers": blockers,
        "actions": actions,
    }


def evaluate_outcomes(
    *,
    qc_report: Dict[str, Any],
    rag_before_score: int | None,
    rag_after: Dict[str, Any] | None,
    duration_ms: int,
    performance_mode: str,
    remove_duplicates_enabled: bool,
) -> Dict[str, Any]:
    missing_raw = _avg(qc_report.get("missing_pct_raw"))
    missing_cleaned = _avg(qc_report.get("missing_pct_cleaned"))
    missing_delta = round(missing_cleaned - missing_raw, 3)
    missing_status = _status_for_threshold(missing_delta, pass_max=2.0, warn_max=5.0)

    invalid_total = _sum_int(qc_report.get("invalid_values"))
    row_count_cleaned = int(qc_report.get("row_count_cleaned") or 0)
    invalid_columns_tracked = len(qc_report.get("invalid_values") or {})
    invalid_rate = round(
        (invalid_total / max(1, row_count_cleaned * max(1, invalid_columns_tracked))) * 100.0,
        3,
    )
    invalid_status = _status_for_threshold(invalid_rate, pass_max=1.0, warn_max=3.0)

    duplicates_removed = int(qc_report.get("duplicate_rows_removed") or 0)
    if remove_duplicates_enabled:
        dedup_status = "pass" if duplicates_removed > 0 else "warn"
        dedup_target = "> 0 when duplicates exist"
        dedup_action = "Enable/verify dedup and inspect candidate key columns."
    else:
        dedup_status = "warn"
        dedup_target = "Dedup enabled for this run"
        dedup_action = "Enable dedup if retrieval quality or storage efficiency matters."

    high_issue_count = _high_severity_count(qc_report.get("issues"))
    critical_status = _status_for_threshold(float(high_issue_count), pass_max=0, warn_max=1)

    rag_score = int((rag_after or {}).get("score") or 0)
    rag_target = rag_before_score if isinstance(rag_before_score, int) else 70
    rag_status = _status_for_threshold(float(rag_score), pass_min=float(rag_target), warn_min=max(60.0, rag_target - 10))

    pii_check_status = None
    for check in (rag_after or {}).get("checks", []):
        if check.get("id") == "pii_safety":
            pii_check_status = (check.get("status") or "").lower()
            break
    if pii_check_status in {"pass", "warn", "fail"}:
        pii_status = pii_check_status
    else:
        pii_status = "warn"

    mode_targets = {
        "balanced": 5 * 60 * 1000,
        "fast": 3 * 60 * 1000,
        "ultra_fast": 2 * 60 * 1000,
    }
    target_ms = mode_targets.get(performance_mode, 5 * 60 * 1000)
    runtime_status = _status_for_threshold(float(duration_ms), pass_max=float(target_ms), warn_max=float(target_ms * 1.5))

    outcomes = [
        _outcome(
            outcome_id="missingness_reduction",
            label="Missingness Health",
            status=missing_status,
            target="Average missingness should not worsen by >2.0 percentage points",
            observed_value=missing_delta,
            unit="percentage_points",
            evidence=f"avg_missing_raw={missing_raw:.3f}, avg_missing_cleaned={missing_cleaned:.3f}",
            recommended_action="Review high-missing columns and imputation/normalization choices.",
        ),
        _outcome(
            outcome_id="invalid_value_reduction",
            label="Invalid Value Rate",
            status=invalid_status,
            target="Invalid rate <=1.0% (warn <=3.0%)",
            observed_value=invalid_rate,
            unit="percent",
            evidence=(
                f"invalid_total={invalid_total}, cleaned_rows={row_count_cleaned}, "
                f"invalid_columns_tracked={invalid_columns_tracked}"
            ),
            recommended_action="Tighten type coercion and normalization for problematic columns.",
        ),
        _outcome(
            outcome_id="duplicate_reduction",
            label="Duplicate Reduction",
            status=dedup_status,
            target=dedup_target,
            observed_value=duplicates_removed,
            unit="rows_removed",
            evidence=f"duplicate_rows_removed={duplicates_removed}, dedup_enabled={remove_duplicates_enabled}",
            recommended_action=dedup_action,
        ),
        _outcome(
            outcome_id="critical_date_integrity",
            label="Critical Integrity Issues",
            status=critical_status,
            target="0 high-severity QC issues",
            observed_value=high_issue_count,
            unit="issue_count",
            evidence=f"high_severity_issues={high_issue_count}",
            recommended_action="Address high-severity date/code/privacy issues before downstream use.",
        ),
        _outcome(
            outcome_id="pii_safety",
            label="PII Safety",
            status=pii_status,
            target="PII safety check should pass",
            observed_value=pii_check_status or "unknown",
            unit="status",
            evidence="Derived from RAG readiness pii_safety check",
            recommended_action="Enable Safe Harbor or strengthen identifier masking rules.",
        ),
        _outcome(
            outcome_id="rag_readiness_score",
            label="RAG Readiness",
            status=rag_status,
            target=f"Post-clean score >= {rag_target}",
            observed_value=rag_score,
            unit="score_0_100",
            evidence=f"rag_before={rag_before_score}, rag_after={rag_score}",
            recommended_action="Improve text density/chunkability and resolve failed RAG checks.",
        ),
        _outcome(
            outcome_id="run_duration_target",
            label="Run Duration",
            status=runtime_status,
            target=f"<= {target_ms} ms for {performance_mode} mode",
            observed_value=duration_ms,
            unit="ms",
            evidence=f"duration_ms={duration_ms}, performance_mode={performance_mode}",
            recommended_action="Use faster mode, larger chunks, or reduce heavy validations for large files.",
        ),
    ]

    status_counts = {"pass": 0, "warn": 0, "fail": 0}
    for outcome in outcomes:
        status = outcome["status"]
        if status not in status_counts:
            status = "warn"
        status_counts[status] += 1

    failed_outcomes = [outcome["id"] for outcome in outcomes if outcome["status"] == "fail"]
    gate_status = _quality_gate_status(failed_outcomes)
    if gate_status == "fail":
        summary = (
            f"{len(failed_outcomes)} failed outcomes, including critical failures. "
            "Release should be blocked until critical outcomes pass."
        )
    elif gate_status == "warn":
        summary = (
            f"{len(failed_outcomes)} failed outcomes (non-critical). "
            "Run is in warn mode; review and remediate before production release."
        )
    else:
        summary = "All tracked outcomes passed or warned within acceptable limits."

    quality_gate = {
        "mode": "warn",
        "status": gate_status,
        "failed_outcomes": failed_outcomes,
        "summary": summary,
    }

    return {
        "items": outcomes,
        "status_counts": status_counts,
        "quality_gate": quality_gate,
    }
