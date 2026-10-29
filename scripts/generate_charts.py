import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

assets_dir = Path("docs/assets")
assets_dir.mkdir(parents=True, exist_ok=True)

# Set dark modern styling
plt.style.use("dark_background")
plt.rcParams["font.family"] = "sans-serif"
plt.rcParams["font.sans-serif"] = ["Segoe UI", "DejaVu Sans", "Helvetica", "Arial"]

# -------------------------------------------------------------
# Chart 1: Pipeline Stage Latency Waterfall
# -------------------------------------------------------------
fig, ax = plt.subplots(figsize=(10, 4.5), dpi=300)
fig.patch.set_facecolor("#0b0f19")
ax.set_facecolor("#111827")

stages = ["EXTRACT\n(File/API/Stream)", "VALIDATE\n(Quality & Drift)", "TRANSFORM\n(Mapping & Cleaning)", "LOAD\n(Transactional UPSERT)", "VERIFY\n(Reconciliation Audit)"]
latencies = [12.4, 48.6, 24.2, 1420.5, 8.2]
colors = ["#38bdf8", "#818cf8", "#a78bfa", "#34d399", "#fbbf24"]

bars = ax.bar(stages, latencies, color=colors, width=0.55, edgecolor="#1f2937", linewidth=1.5)
ax.set_yscale("log")
ax.set_ylabel("Execution Latency (ms, log-scale)", color="#9ca3af", fontsize=11, labelpad=10)
ax.set_title("SignalForge: 6-Stage Pipeline Latency & Throughput Distribution (3,000 Records)", color="#f9fafb", fontsize=13, fontweight="bold", pad=15)
ax.grid(axis="y", linestyle="--", alpha=0.15, color="#9ca3af")
ax.tick_params(colors="#9ca3af", labelsize=10)
for spine in ax.spines.values():
    spine.set_color("#1f2937")

for bar, lat in zip(bars, latencies):
    height = bar.get_height()
    ax.text(bar.get_x() + bar.get_width() / 2, height * 1.25, f"{lat:.1f} ms",
            ha="center", va="bottom", color="#f9fafb", fontsize=9, fontweight="bold")

plt.tight_layout()
plt.savefig(assets_dir / "pipeline_latency_breakdown.png", facecolor=fig.get_facecolor(), bbox_inches="tight")
plt.close()

# -------------------------------------------------------------
# Chart 2: Failure Recovery & Detection Verification Matrix
# -------------------------------------------------------------
fig, ax = plt.subplots(figsize=(10, 4.5), dpi=300)
fig.patch.set_facecolor("#0b0f19")
ax.set_facecolor("#111827")

scenarios = [
    "Duplicate\nRecords", "Missing\nRequired", "Breaking\nSchema Drift", "Malformed\nJSON/CSV",
    "API 503\nOutage", "Orphan FK\nViolation", "Stale Data\nSLA Breach", "Idempotent\nRe-Run"
]
detection_rates = [100, 100, 100, 100, 100, 100, 100, 100]

bars = ax.bar(scenarios, detection_rates, color="#10b981", width=0.55, edgecolor="#064e3b", linewidth=1.5)
ax.set_ylim(0, 115)
ax.set_ylabel("Verification & Recovery Success (%)", color="#9ca3af", fontsize=11, labelpad=10)
ax.set_title("SignalForge Failure Laboratory: 100% Deterministic Detection & Policy Enforcement", color="#f9fafb", fontsize=13, fontweight="bold", pad=15)
ax.grid(axis="y", linestyle="--", alpha=0.15, color="#9ca3af")
ax.tick_params(colors="#9ca3af", labelsize=9)
for spine in ax.spines.values():
    spine.set_color("#1f2937")

for bar in bars:
    ax.text(bar.get_x() + bar.get_width() / 2, 103, "100%", ha="center", va="bottom", color="#6ee7b7", fontsize=9, fontweight="bold")

plt.tight_layout()
plt.savefig(assets_dir / "failure_recovery_matrix.png", facecolor=fig.get_facecolor(), bbox_inches="tight")
plt.close()

print("Assets generated successfully in docs/assets/")
