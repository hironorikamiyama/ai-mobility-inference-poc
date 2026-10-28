#!/usr/bin/env python3
"""Phase 23: Visualize recovery measurements and existing SLO evaluations."""

import argparse
import html
import json
import math
import statistics
import sys
from datetime import datetime
from pathlib import Path

METRICS = {
    "pod_recreated_observed": "Pod recreation observed",
    "pod_ready_observed": "Pod Ready observed",
    "job_recovered_observed": "Job recovery observed",
    "ready_to_recovery": "Ready to recovery",
}
PHASE18_PATTERN = "phase18-*.json"
ANALYSIS_NAME = "recovery-analysis.json"
HTML_NAME = "recovery-analysis.html"


def load_json(path):
    with path.open(encoding="utf-8") as file:
        return json.load(file)


def json_safe(value):
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, list):
        return [json_safe(item) for item in value]
    if isinstance(value, dict):
        return {key: json_safe(item) for key, item in value.items()}
    return value


def parse_timestamp(value):
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed


def valid_seconds(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        number = float(value)
    except OverflowError:
        return None
    if not math.isfinite(number) or number < 0:
        return None
    return number


def load_measurements(directory):
    records = []
    for path in sorted(directory.glob(PHASE18_PATTERN)):
        record = load_json(path)
        if not isinstance(record, dict):
            raise ValueError(f"Measurement JSON must be an object: {path}")
        records.append({
            "source": path.name,
            "job_id": record.get("job_id"),
            "status": record.get("status", "unknown"),
            "observed_at": record.get("observed_at"),
            "measurements_seconds": {
                metric: valid_seconds(
                    record.get("measurements_seconds", {}).get(metric)
                )
                if isinstance(record.get("measurements_seconds"), dict)
                else None
                for metric in METRICS
            },
            "measurement_conditions": json_safe(
                record.get("measurement_conditions")
            ),
            "measurement_conditions_comparable": record.get(
                "measurement_conditions_comparable"
            ),
            "measurement_conditions_reason": record.get(
                "measurement_conditions_reason"
            ),
        })
    return records


def sorted_measurements(records):
    timestamps = [parse_timestamp(record["observed_at"]) for record in records]
    if records and all(timestamp is not None for timestamp in timestamps):
        ordered = [
            record
            for _, record in sorted(
                zip(timestamps, records),
                key=lambda pair: pair[0],
            )
        ]
        return ordered, "timestamp"
    return records, "unknown"


def summarize_conditions(records, phase19_summary):
    comparable = [
        record for record in records
        if record.get("status") == "passed"
    ]
    if not comparable:
        return {
            "comparable": False,
            "conditions": None,
            "reason": "No successful measurements are available.",
        }

    reference = None
    reasons = []
    for record in comparable:
        conditions = record.get("measurement_conditions")
        if record.get("measurement_conditions_comparable") is not True:
            reasons.append(
                f"{record.get('job_id') or record['source']}: "
                f"{record.get('measurement_conditions_reason') or 'conditions are not verified comparable'}"
            )
            continue
        if not isinstance(conditions, dict):
            reasons.append(
                f"{record.get('job_id') or record['source']}: "
                "measurement_conditions is missing"
            )
            continue
        if reference is None:
            reference = conditions
        elif conditions != reference:
            reasons.append(
                f"{record.get('job_id') or record['source']}: "
                "measurement conditions differ from other successful records"
            )

    phase19_comparable = (
        phase19_summary.get("measurement_conditions_comparable")
        if isinstance(phase19_summary, dict)
        else None
    )
    phase19_reason = (
        phase19_summary.get("measurement_conditions_reason")
        if isinstance(phase19_summary, dict)
        else None
    )
    if phase19_comparable is False:
        reasons.append(
            phase19_reason
            if isinstance(phase19_reason, str) and phase19_reason
            else "Phase 19 marked the conditions as incomparable."
        )
    elif isinstance(phase19_summary, dict) and phase19_comparable is not True:
        reasons.append("Phase 19 comparability was not established.")

    is_comparable = not reasons and reference is not None
    return {
        "comparable": is_comparable,
        "conditions": reference if is_comparable else None,
        "reason": (
            "All successful records have known, identical measurement conditions."
            if is_comparable
            else "; ".join(dict.fromkeys(reasons))
        ),
    }


def calculate_statistics(records):
    result = {}
    for metric in METRICS:
        values = []
        for record in records:
            if record.get("status") != "passed":
                continue
            measurements = record.get("measurements_seconds")
            value = (
                measurements.get(metric)
                if isinstance(measurements, dict)
                else None
            )
            number = valid_seconds(value)
            if number is not None:
                values.append(number)
        result[metric] = {
            "count": len(values),
            "mean": statistics.mean(values) if values else None,
            "median": statistics.median(values) if values else None,
            "min": min(values) if values else None,
            "max": max(values) if values else None,
            "stdev": statistics.stdev(values) if len(values) > 1 else None,
        }
    return result


def phase19_statistics(summary):
    metrics = summary.get("metrics") if isinstance(summary, dict) else None
    if not isinstance(metrics, dict):
        return None
    result = {}
    for metric in METRICS:
        source = metrics.get(metric)
        if source is None:
            result[metric] = {
                "count": 0,
                "mean": None,
                "median": None,
                "min": None,
                "max": None,
                "stdev": None,
            }
            continue
        if not isinstance(source, dict):
            return None
        result[metric] = {
            key: valid_seconds(source.get(key))
            for key in ("mean", "median", "min", "max", "stdev")
        }
        result[metric]["count"] = (
            summary.get("passed_runs")
            if isinstance(summary.get("passed_runs"), int)
            and not isinstance(summary.get("passed_runs"), bool)
            else 0
        )
    return result


def build_analysis(records, phase19_summary, phase20_report, phase22_result):
    ordered, ordering = sorted_measurements(records)
    succeeded = sum(record.get("status") == "passed" for record in records)
    failed = sum(record.get("status") == "failed" for record in records)
    from_phase19 = phase19_statistics(phase19_summary)
    stats = from_phase19 or calculate_statistics(records)
    return {
        "report": "Phase 23 recovery analysis",
        "measurement_count": len(records),
        "successful_count": succeeded,
        "failed_count": failed,
        "ordering": ordering,
        "measurements": ordered,
        "statistics": stats,
        "statistics_source": (
            "phase19" if from_phase19 is not None else "phase18"
        ),
        "measurement_conditions": summarize_conditions(
            records,
            phase19_summary,
        ),
        "slo_evaluation": phase20_report,
        "phase22_result": phase22_result,
        "limitations": [
            "Recovery time includes the 30-second test-only pause when recorded in the measurement conditions.",
            "Configured thresholds are illustrative PoC values, not production SLOs.",
            "Three or fewer samples are insufficient for a reliable p95 or SLO guarantee.",
            "Missing timestamps or measurement conditions are not inferred.",
            "Invalid, missing, and failed-record measurements are excluded from fallback statistics.",
        ],
    }


def display_number(value):
    number = valid_seconds(value)
    return "N/A" if number is None else f"{number:.3f}"


def metric_chart(metric, label, measurements, evaluation, ordering):
    values = []
    for record in measurements:
        if record.get("status") != "passed":
            continue
        measurement_map = record.get("measurements_seconds")
        value = (
            valid_seconds(measurement_map.get(metric))
            if isinstance(measurement_map, dict)
            else None
        )
        if value is not None:
            values.append((record, value))

    width = 720
    height = 232
    left = 60
    right = 18
    top = 24
    bottom = 64
    plot_width = width - left - right
    plot_height = height - top - bottom
    metric_evaluation = (
        evaluation.get("metrics", {}).get(metric)
        if isinstance(evaluation, dict)
        and isinstance(evaluation.get("metrics"), dict)
        else None
    )
    threshold = (
        valid_seconds(metric_evaluation.get("threshold_seconds"))
        if isinstance(metric_evaluation, dict)
        else None
    )
    maximum = max(
        [value for _, value in values] + ([threshold] if threshold is not None else []) + [1]
    )
    bar_slot = plot_width / max(len(values), 1)
    bar_width = min(36, bar_slot * 0.6)
    parts = [
        f'<svg viewBox="0 0 {width} {height}" role="img" '
        f'aria-label="{html.escape(label, quote=True)} recovery time chart">',
        f'<line x1="{left}" y1="{top + plot_height}" x2="{width-right}" '
        f'y2="{top + plot_height}" stroke="#64748b"/>',
        f'<text x="{left-8}" y="{top+5}" text-anchor="end" '
        'font-size="13" fill="#475569">s</text>',
    ]
    if threshold is not None:
        y = top + plot_height - (threshold / maximum) * plot_height
        parts.extend([
            f'<line x1="{left}" y1="{y:.1f}" x2="{width-right}" '
            f'y2="{y:.1f}" stroke="#dc2626" stroke-dasharray="5 4"/>',
            f'<text x="{width-right-2}" y="{y-4:.1f}" text-anchor="end" '
            'font-size="14" font-weight="600" fill="#b91c1c">'
            f'Threshold {threshold:g}s</text>',
        ])
    for index, (record, value) in enumerate(values):
        x = left + index * bar_slot + (bar_slot - bar_width) / 2
        bar_height = value / maximum * plot_height
        y = top + plot_height - bar_height
        parts.extend([
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_width:.1f}" '
            f'height="{bar_height:.1f}" rx="3" fill="#2563eb"/>',
            f'<text x="{x + bar_width/2:.1f}" y="{y-4:.1f}" '
            f'text-anchor="middle" font-size="13" font-weight="600" '
            f'fill="#1e293b">'
            f'{value:.2f}</text>',
        ])
        if ordering == "timestamp":
            label_text = str(record["observed_at"])[:16]
        else:
            label_text = (
                record.get("job_id")
                if isinstance(record.get("job_id"), str)
                else f"Record {index + 1}"
            )
        if len(label_text) > 13:
            label_text = label_text[:12] + "…"
        parts.append(
            f'<text x="{x + bar_width/2:.1f}" y="{height-18}" '
            f'text-anchor="end" transform="rotate(-25 '
            f'{x + bar_width/2:.1f} {height-18})" font-size="12" '
            f'fill="#475569">{html.escape(label_text)}</text>'
        )
    if not values:
        parts.append(
            f'<text x="{width/2}" y="{height/2}" text-anchor="middle" '
            'fill="#64748b">No valid successful measurements</text>'
        )
    parts.append("</svg>")
    return "\n".join(parts)


def render_html(analysis):
    evaluation = analysis.get("slo_evaluation")
    overall_status = (
        evaluation.get("overall_status", "Unavailable")
        if isinstance(evaluation, dict)
        else "Unavailable"
    )
    phase22 = analysis.get("phase22_result")
    phase22_status = (
        phase22.get("status", "Unavailable")
        if isinstance(phase22, dict)
        else "Unavailable"
    )
    order_note = (
        "Measurements are ordered by recorded timestamp."
        if analysis["ordering"] == "timestamp"
        else "Measurement order is unknown; file names are not treated as chronology."
    )
    condition_data = analysis["measurement_conditions"]
    condition_text = json.dumps(
        condition_data.get("conditions"),
        ensure_ascii=False,
        sort_keys=True,
    )
    condition_reason = condition_data.get("reason", "")
    stats = analysis["statistics"]

    metric_sections = []
    for metric, label in METRICS.items():
        aggregate = stats[metric]
        evaluation_metric = (
            evaluation.get("metrics", {}).get(metric)
            if isinstance(evaluation, dict)
            and isinstance(evaluation.get("metrics"), dict)
            else None
        )
        status = (
            evaluation_metric.get("status", "Unavailable")
            if isinstance(evaluation_metric, dict)
            else "Unavailable"
        )
        reason = (
            evaluation_metric.get("reason", "")
            if isinstance(evaluation_metric, dict)
            else "No Phase 20 evaluation data is available."
        )
        evaluation_data = evaluation if isinstance(evaluation, dict) else {}
        chart = metric_chart(
            metric,
            label,
            analysis["measurements"],
            evaluation_data,
            analysis["ordering"],
        )
        comparability_note = (
            ""
            if condition_data["comparable"]
            else (
                "<p class=\"warning\">Individual observations only; "
                "conditions are not comparable, so no cross-run trend "
                "or SLO conclusion is drawn here. Any available aggregate "
                "statistics are reported from Phase 19 without treating "
                "different conditions as a comparable trend.</p>"
            )
        )
        metric_sections.append(
            "<section class=\"metric\">"
            f"<h3>{html.escape(label)}</h3>"
            f"<p>Evaluation: <strong>{html.escape(str(status))}</strong> "
            f"— {html.escape(str(reason))}</p>"
            f"{comparability_note}{chart}"
            "<table><tbody>"
            f"<tr><th>Count</th><td>{html.escape(str(aggregate['count']))}</td></tr>"
            f"<tr><th>Mean (s)</th><td>{display_number(aggregate['mean'])}</td></tr>"
            f"<tr><th>Median (s)</th><td>{display_number(aggregate['median'])}</td></tr>"
            f"<tr><th>Min (s)</th><td>{display_number(aggregate['min'])}</td></tr>"
            f"<tr><th>Max (s)</th><td>{display_number(aggregate['max'])}</td></tr>"
            f"<tr><th>Std dev (s)</th><td>{display_number(aggregate['stdev'])}</td></tr>"
            "</tbody></table></section>"
        )

    limitations = "".join(
        f"<li>{html.escape(item)}</li>"
        for item in analysis["limitations"]
    )
    condition_status = (
        "Comparable" if condition_data["comparable"] else "Not comparable"
    )
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Phase 23 Recovery Analysis</title>
<style>
body{{font-family:system-ui,sans-serif;max-width:1100px;margin:2rem auto;padding:0 1rem;color:#172033;background:#f8fafc}}
h1,h2,h3{{color:#0f172a}} .overview,.metric{{box-sizing:border-box;min-width:0;background:white;border:1px solid #dbe3ee;border-radius:10px;padding:1rem;margin:1rem 0}}
.grid{{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:1rem;align-items:start}} svg{{display:block;width:100%;height:auto;overflow:visible}}
table{{border-collapse:collapse;width:100%}} th,td{{text-align:left;padding:.3rem;border-bottom:1px solid #e2e8f0}}
.note,.warning{{color:#475569}} pre{{max-width:100%;box-sizing:border-box;white-space:pre-wrap;overflow-wrap:anywhere;word-break:break-word}} code{{overflow-wrap:anywhere;word-break:break-word}}
@media(max-width:700px){{body{{margin:1rem auto}} .grid{{grid-template-columns:minmax(0,1fr)}}}}
</style>
</head>
<body>
<h1>Phase 23: Recovery Analysis</h1>
<section class="overview">
<h2>Measurement overview</h2>
<p>Measurements: {analysis['measurement_count']} |
Successful: {analysis['successful_count']} | Failed: {analysis['failed_count']}</p>
<p>Phase 22 status: <strong>{html.escape(str(phase22_status))}</strong> |
Phase 20 evaluation: <strong>{html.escape(str(overall_status))}</strong></p>
<p>{html.escape(order_note)}</p>
<p>Statistics source: {html.escape(str(analysis['statistics_source']))}</p>
</section>
<h2>Recovery time metrics</h2>
<div class="grid">{''.join(metric_sections)}</div>
<section class="overview">
<h2>Measurement conditions</h2>
<p>Status: <strong>{html.escape(condition_status)}</strong></p>
<p>{html.escape(condition_reason)}</p>
<pre><code>{html.escape(condition_text)}</code></pre>
</section>
<section class="overview">
<h2>Limitations and cautions</h2><ul>{limitations}</ul>
</section>
<p class="note">Generated from available Phase 18–22 artifacts. No missing dates, conditions, or evaluations have been inferred.</p>
</body>
</html>
"""


def path_overlaps(first, second):
    first = first.resolve()
    second = second.resolve()
    return first == second or first in second.parents or second in first.parents


def generate(input_dir, output_dir, measurement_dir=None):
    if output_dir.exists() or output_dir.is_symlink():
        raise FileExistsError(
            f"Output directory already exists: {output_dir}"
        )
    input_dir = input_dir.resolve()
    output_resolved = output_dir.resolve()
    if path_overlaps(input_dir, output_resolved):
        raise ValueError("Input and output directories must not overlap.")
    if not input_dir.is_dir():
        raise FileNotFoundError(f"Phase 22 input directory does not exist: {input_dir}")
    measurement_dir = (
        measurement_dir.resolve()
        if measurement_dir is not None
        else input_dir / "phase18"
    )
    if (
        measurement_dir != input_dir / "phase18"
        and not measurement_dir.is_dir()
    ):
        raise FileNotFoundError(
            f"Phase 18 measurement directory does not exist: {measurement_dir}"
        )
    if measurement_dir != input_dir / "phase18" and path_overlaps(
        measurement_dir,
        output_resolved,
    ):
        raise ValueError(
            "Phase 18 input and output directories must not overlap."
        )
    records = load_measurements(measurement_dir) if measurement_dir.is_dir() else []
    phase19_path = input_dir / "phase19" / "summary.json"
    phase20_path = input_dir / "phase20" / "slo-evaluation.json"
    phase22_path = input_dir / "phase22-result.json"
    phase19_summary = (
        json_safe(load_json(phase19_path))
        if phase19_path.is_file()
        else None
    )
    phase20_report = (
        json_safe(load_json(phase20_path))
        if phase20_path.is_file()
        else None
    )
    phase22_result = (
        json_safe(load_json(phase22_path))
        if phase22_path.is_file()
        else None
    )
    analysis = build_analysis(
        records,
        phase19_summary,
        phase20_report,
        phase22_result,
    )

    output_dir.mkdir(parents=True, exist_ok=False)
    (output_dir / ANALYSIS_NAME).write_text(
        json.dumps(analysis, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    (output_dir / HTML_NAME).write_text(
        render_html(analysis),
        encoding="utf-8",
    )
    return analysis


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Generate a static HTML and JSON recovery analysis from Phase 22 output."
    )
    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Phase 22 output directory to analyze",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="New directory for Phase 23 reports; existing paths are rejected",
    )
    parser.add_argument(
        "--phase18-dir",
        type=Path,
        help="Optional Phase 18 measurement directory (needed for offline Phase 22 output)",
    )
    args = parser.parse_args(argv)
    try:
        analysis = generate(args.input, args.output, args.phase18_dir)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"Phase 23 analysis failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps({
        "measurement_count": analysis["measurement_count"],
        "successful_count": analysis["successful_count"],
        "failed_count": analysis["failed_count"],
        "output": str(args.output),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
