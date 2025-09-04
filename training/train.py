import pandas as pd
import numpy as np
from autogluon.timeseries import TimeSeriesDataFrame, TimeSeriesPredictor
import os
from collections import defaultdict

import warnings
warnings.filterwarnings("ignore")

trace_path = '../traces/trace_eastus.20241101-14.csv'

trained_models_path = './trained_models/'
os.makedirs(trained_models_path, exist_ok=True)

# Aggregate the data to hourly frequency
freq = "3600S"

# One day
prediction_length = 1 * 24 


models = ["WeightedEnsemble", "SeasonalNaive", "DeepAR", "TemporalFusionTransformer", "PatchTST", "AutoETS", "Theta", "AutoARIMA", "Chronos[autogluon__chronos-bolt-small]"]
dfs = {"WeightedEnsemble": None, "SeasonalNaive": None, "DeepAR": None, "TemporalFusionTransformer": None, "PatchTST": None, "AutoETS": None, "Theta": None, "AutoARIMA": None, "Chronos[autogluon__chronos-bolt-small]": None}



# Read and clean the trace
columns = ['timestamp', 'value1', 'value2', 'operation', 'value3', 'value4', 'size', 'value5', 'value6', 'value7', 'value8', 'runtime', 'runtime_version']
df = pd.read_csv(trace_path, header=None, names=columns)
df['operation'] = df['operation'].str.strip()
df = df[df['operation'] == 'Allocate']
df = df.drop(columns=['value1', 'value2', 'value3', 'value4', 'value5', 'value6', 'value7', 'value8'])
df['runtime_version'] = df['runtime_version'].str.replace('--cores=0.25', '', regex=False)
df['timestamp'] = pd.to_datetime(df['timestamp'])
df['count'] = 1


df["timestamp"] = pd.to_datetime(df["timestamp"])
global_start = df['timestamp'].min()
global_start = global_start.floor(freq=freq)


runtime_versions = df.groupby(['runtime', 'runtime_version', 'size']).size().reset_index()

# runtime_versions = runtime_versions.head(1)

print("Available runtime and version combinations:")
print(runtime_versions)

all = df.copy()

# ===================== Train models for each runtime/version/size combo with a rolling fashion ======================
for runtime, version, size in runtime_versions[['runtime', 'runtime_version', 'size']].itertuples(index=False):
    print(f"Training model for {runtime} {version} {size}")
    # one week
    for i in range(7):
        key = f"{runtime.strip()}_{version.strip()}_{size.strip()}"
        if os.path.exists(trained_models_path+"OneHour_"+key+"_"+str(i)):
            print(f"Skipping {key} window {i} - model already exists")
            continue
        df = all.copy()
        temp = df[(df['runtime'] == runtime) & (df['runtime_version'] == version) & (df['size'] == size)]
        temp['timestamp'] = temp['timestamp'].dt.floor(freq=freq)
        temp = temp.groupby('timestamp', as_index=False).agg({'count': 'sum'})
        temp = temp.rename(columns={'count': 'allocations'})
        temp.set_index('timestamp', inplace=True)
        full_range = pd.date_range(start=global_start, end=temp.index.max(), freq=freq)
        temp = temp.reindex(full_range)
        temp.fillna(0, inplace=True)
        temp = temp.reset_index().rename(columns={'index': 'timestamp'})
        temp["item_id"] = key
        temp = temp[temp["timestamp"] < (global_start + pd.DateOffset(days=8+i))]
        temp = temp[temp["timestamp"] >= (global_start + pd.DateOffset(days=i))]
        
        tsdf = TimeSeriesDataFrame.from_data_frame(temp, timestamp_column='timestamp', id_column='item_id')
        train_data, test_data = tsdf.train_test_split(prediction_length)
        
        predictor = TimeSeriesPredictor(
        prediction_length=prediction_length,
        path= trained_models_path+"OneHour_"+key+"_"+str(i),
        target="allocations",
        eval_metric_seasonal_period=24,
        eval_metric="MAPE",
        )

        predictor.fit(
            train_data,
            presets="high_quality",
            hyperparameters={
                "SeasonalNaive": {"season_length": 24},
                "DeepAR": {},
                "TemporalFusionTransformer": {},
                "PatchTST": {},
                "AutoETS": {},
                "Theta": {},
                "AutoARIMA": {},
                "Chronos":{},
            }
            ,
            time_limit=3600,
        )

# ===================== Generate predicted trace & evaluation  =====================


# --- helper: build hourly series for one key, matching training aggregation ---
def build_hourly_series(all_df, runtime, version, size, freq="3600S"):
    key = f"{runtime.strip()}_{version.strip()}_{size.strip()}"
    temp = all_df[(all_df['runtime'] == runtime) &
                  (all_df['runtime_version'] == version) &
                  (all_df['size'] == size)].copy()

    # Hourly floor + count == "allocations", fill missing hours with 0
    temp['timestamp'] = temp['timestamp'].dt.floor(freq=freq)
    temp = temp.groupby('timestamp', as_index=False).agg({'count': 'sum'})
    temp = temp.rename(columns={'count': 'allocations'})
    temp.set_index('timestamp', inplace=True)
    full_range = pd.date_range(start=global_start, end=temp.index.max(), freq=freq)
    temp = temp.reindex(full_range)
    temp.fillna(0, inplace=True)
    temp = temp.reset_index().rename(columns={'index': 'timestamp'})
    temp['item_id'] = key
    return key, temp

# --- helper: sanitize model name for filesystem ---
def sanitize_model_name(name: str) -> str:
    safe = "".join(c if c.isalnum() or c in "-._[]" else "_" for c in name)
    return safe

# Freeze a clean copy of the full 14-day filtered data (named "all" above)
all_data = all.copy()

# Accumulators
merged_predictions = defaultdict(list)   # (key, model) -> list of merged dfs across 7 windows
metrics_rows = []                        # list of dicts for metrics table
train_predict_times = defaultdict(float) # (key, model) -> total fit+predict time over windows

# Where to save artifacts
predictions_root = "predictions_hourly"
os.makedirs(predictions_root, exist_ok=True)
best_models_dir = "best_models"
os.makedirs(best_models_dir, exist_ok=True)

# Iterate all runtime/version/size combos discovered earlier
for runtime, version, size in runtime_versions[['runtime', 'runtime_version', 'size']].itertuples(index=False):
    print(f"\n[INFO] Processing {runtime} {version} {size}")
    key, full_series = build_hourly_series(all_data, runtime, version, size, freq=freq)

    # Skip keys that don't have enough data for 8+ days windows
    if full_series.empty:
        print(f"[WARN] No data for {key}, skipping.")
        continue

    # For each sliding window i=0..6 used in training
    for i in range(7):
        # Reproduce the training window slicing exactly as above
        temp = full_series.copy()
        start_cut = temp["timestamp"].min() + pd.DateOffset(days=i)
        end_cut   = temp["timestamp"].min() + pd.DateOffset(days=8 + i)
        temp = temp[(temp["timestamp"] >= start_cut) & (temp["timestamp"] < end_cut)]
        if temp["timestamp"].nunique() < (prediction_length + 1):  # need at least pred_length + 1 timestamps
            print(f"[WARN] Not enough timestamps for window {i} on {key}, skipping window.")
            continue
        
        tsdf = TimeSeriesDataFrame.from_data_frame(temp, timestamp_column='timestamp', id_column='item_id')
        train_data, test_data = tsdf.train_test_split(prediction_length)

        # Load the predictor trained for this key + window
        model_path = f"{trained_models_path}OneHour_{key}_{i}"
        if not os.path.exists(model_path):
            print(f"[WARN] Predictor path not found: {model_path} (skipping).")
            continue
        try:
            predictor = TimeSeriesPredictor.load(model_path)
        except Exception as e:
            print(f"[WARN] Failed to load predictor at {model_path}: {e} (skipping).")
            continue
        info = predictor.info()
        model_names = list(info.get("model_info", {}).keys())
        if not model_names:
            print(f"[WARN] No models found in predictor {model_path}.")
            continue

        # Prepare test frame to merge against predictions
        # AFTER (robust across AG versions)
        test_df = test_data.reset_index()
        # make sure we know the target column name
        target_col = getattr(predictor, "target", None) or "allocations"
        if target_col not in test_df.columns:
            # fallback: grab the single non-index value column
            value_cols = [c for c in test_df.columns if c not in ("item_id", "timestamp")]
            if len(value_cols) == 1:
                target_col = value_cols[0]
        # standardize as 'allocations' for downstream code
        test_df = test_df.rename(columns={target_col: "allocations"})[["item_id", "timestamp", "allocations"]]
        
        # Predict with each available model in this predictor
        for model_name in model_names:
            try:
                preds = predictor.predict(train_data, model=model_name).reset_index()  # has 'mean' + quantiles
            except Exception as e:
                print(f"[WARN] Prediction failed for {key} / window {i} / model {model_name}: {e}")
                continue

            # Merge predictions with the test ground truth
            merged_df = pd.merge(
                preds, test_df, on=["item_id", "timestamp"], how="inner", validate="one_to_one"
            )
            merged_df["window"] = i
            merged_df["model"] = model_name

            # Collect per (key, model)
            merged_predictions[(key, model_name)].append(merged_df)

            # Track (fit + predict) time aggregates, if available
            mi = info["model_info"].get(model_name, {})
            fit_t = float(mi.get("fit_time", 0.0) or 0.0)
            pred_t = float(mi.get("predict_time", 0.0) or 0.0)
            train_predict_times[(key, model_name)] += (fit_t + pred_t)

    # After finishing all windows for this key, write per-model merged traces to CSVs
    for model_key, parts in list(merged_predictions.items()):
        k, m = model_key  # model_key is the (key, model) tuple
        if k != key:
            continue
        if not parts:
            continue
        out_df = pd.concat(parts, ignore_index=True).sort_values(["timestamp", "window"])
        # Save under predictions_root / key / <model>.csv
        key_dir = os.path.join(predictions_root, key)
        os.makedirs(key_dir, exist_ok=True)
        out_path = os.path.join(key_dir, f"{sanitize_model_name(m)}.csv")
        out_df.to_csv(out_path, index=False)

# ---------------- Compute metrics per key & model ----------------
metrics = []
for (key, model_name), parts in merged_predictions.items():
    if not parts:
        continue
    dfm = pd.concat(parts, ignore_index=True)
    # Only evaluate on non-zero actuals to avoid divide-by-zero for MAPE
    df_eval = dfm[dfm["allocations"] != 0].copy()
    if df_eval.empty:
        print(f"[INFO] All-zero actuals for {key} / {model_name}; skipping metrics.")
        continue
    df_eval["error"] = df_eval["mean"] - df_eval["allocations"]
    df_eval["abs_error"] = df_eval["error"].abs()
    df_eval["pct_error"] = df_eval["error"] / df_eval["allocations"]
    df_eval["abs_pct_error"] = df_eval["pct_error"].abs()

    mape = df_eval["abs_pct_error"].mean() * 100.0
    bias = df_eval["error"].mean()
    max_ape = df_eval["abs_pct_error"].max() * 100.0
    n_points = len(df_eval)

    print(f"\n=== {key} ===")
    print(f"{model_name}")
    print(f"MAPE: {mape:.2f}%")
    print(f"Bias: {bias:.4f}  (negative => under-prediction on average, positive => over)")
    print(f"Max APE: {max_ape:.2f}% over {n_points} evaluated points")

    metrics.append({
        "key": key,
        "model": model_name,
        "mape_percent": mape,
        "bias": bias,
        "max_ape_percent": max_ape,
        "n_points": n_points,
        "total_fit_plus_predict_time_sec": train_predict_times.get((key, model_name), float("nan")),
    })

# Save metrics table
metrics_df = pd.DataFrame(metrics).sort_values(["key", "mape_percent", "model"])
# metrics_csv = "metrics_per_key_model.csv"
# metrics_df.to_csv(metrics_csv, index=False)
# print(f"\n[INFO] Saved metrics: {metrics_csv}")

# ---------------- Pick best model per key (by lowest MAPE) ----------------
best_rows = []
for key, group in metrics_df.groupby("key", sort=False):
    best = group.sort_values(["mape_percent", "max_ape_percent", "bias"]).iloc[0]
    best_rows.append(best)

best_df = pd.DataFrame(best_rows).reset_index(drop=True)
# best_csv = "best_models_per_key.csv"
# best_df.to_csv(best_csv, index=False)
# print(f"[INFO] Saved best-models summary: {best_csv}")

# ---------------- Save merged traces for best model per key into ./best_models ----------------
combined_parts = []

for _, row in best_df.iterrows():
    key = row["key"]
    model_name = row["model"]
    parts = merged_predictions.get((key, model_name), [])
    if not parts:
        continue

    out_df = pd.concat(parts, ignore_index=True).sort_values(["timestamp", "window"])
    fname = f"{key}__{sanitize_model_name(model_name)}.csv"
    out_path = os.path.join(best_models_dir, fname)
    out_df.to_csv(out_path, index=False)
    print(f"[INFO] Wrote best predictions for {key} ({model_name}) -> {out_path}")

    # add to combined
    combined_parts.append(out_df)

# write combined file with all pools' best-model predictions
if combined_parts:
    combined_df = pd.concat(combined_parts, ignore_index=True)\
                    .sort_values(["item_id", "timestamp", "window"])
    combined_path = os.path.join(best_models_dir, "predicited_trace.csv")
    combined_df = combined_df.drop(columns=["window"])
    combined_df.to_csv(combined_path, index=False)
    print(f"[INFO] Wrote combined best-model predictions -> {combined_path}")
else:
    print("[INFO] No best-model predictions to combine.")


# times_rows = [
#     {"item_id": k, "model": m, "total_fit_plus_predict_time_sec": t}
#     for (k, m), t in train_predict_times.items()
# ]

# if times_rows:
#     times_df = pd.DataFrame(times_rows).sort_values(["item_id", "total_fit_plus_predict_time_sec"])
#     times_csv = "model_fit_predict_times.csv"
#     times_df.to_csv(times_csv, index=False)
#     print(f"[INFO] Saved fit+predict time aggregates: {times_csv}")

