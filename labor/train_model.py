"""
Sales Forecasting Engine — Step 2: LightGBM Model Training
Trains a global model across all stores with store embeddings via categorical feature.
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
import pickle
import json
from datetime import timedelta
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error

# Load feature matrix
print('Loading feature matrix...')
df = pd.read_pickle('C:/Users/BretElliott/ramz-accounting/labor/feature_matrix.pkl')
print(f'  {len(df):,} rows, {df["location_id"].nunique()} stores')

# Encode categoricals
df['location_id_cat'] = df['location_id'].astype('category')
df['location_type_cat'] = df['location_type'].astype('category')

# --- Train/validation split: hold out last 8 weeks per store for back-test ---
cutoff_date = df['sale_date'].max() - timedelta(weeks=8)
train_df = df[df['sale_date'] <= cutoff_date].copy()
val_df   = df[df['sale_date'] >  cutoff_date].copy()
print(f'  Train: {len(train_df):,} rows | Validation (last 8wk): {len(val_df):,} rows')

# Feature columns
EXCLUDE = {'location_id', 'sale_date', 'net_sales', 'location_type'}
feature_cols = [c for c in df.columns if c not in EXCLUDE]

X_train = train_df[feature_cols]
y_train = train_df['net_sales']
X_val   = val_df[feature_cols]
y_val   = val_df['net_sales']

# LightGBM dataset
cat_features = ['location_id_cat', 'location_type_cat']
train_data = lgb.Dataset(X_train, label=y_train, categorical_feature=cat_features, free_raw_data=False)
val_data   = lgb.Dataset(X_val,   label=y_val,   categorical_feature=cat_features, reference=train_data, free_raw_data=False)

# Model params
params = {
    'objective': 'regression',
    'metric': ['mae', 'mape'],
    'boosting_type': 'gbdt',
    'num_leaves': 63,
    'learning_rate': 0.05,
    'feature_fraction': 0.8,
    'bagging_fraction': 0.8,
    'bagging_freq': 5,
    'min_child_samples': 20,
    'lambda_l1': 0.1,
    'lambda_l2': 0.1,
    'verbose': -1,
    'n_jobs': -1,
}

print('\nTraining LightGBM model...')
callbacks = [lgb.early_stopping(50, verbose=False), lgb.log_evaluation(100)]
model = lgb.train(
    params,
    train_data,
    num_boost_round=1000,
    valid_sets=[train_data, val_data],
    valid_names=['train', 'val'],
    callbacks=callbacks,
)

print(f'  Best iteration: {model.best_iteration}')

# --- Evaluate ---
print('\n=== MODEL EVALUATION (last 8 weeks held out) ===')
val_df = val_df.copy()
val_df['predicted'] = model.predict(X_val)
val_df['predicted'] = val_df['predicted'].clip(lower=0)

# Filter out closed days for evaluation
val_open = val_df[val_df['net_sales'] > 0]

overall_mae  = mean_absolute_error(val_open['net_sales'], val_open['predicted'])
overall_mape = mean_absolute_percentage_error(val_open['net_sales'], val_open['predicted'])
print(f'  Overall MAE:  ${overall_mae:,.0f}')
print(f'  Overall MAPE: {overall_mape*100:.1f}%')

# Per-store accuracy
print('\n  Per-store (open days only):')
print(f'  {"Store":<35} {"MAE":>8} {"MAPE":>8} {"Days":>6}')
print('  ' + '-'*60)
store_results = []
for loc_id, grp in val_open.groupby('location_id'):
    store_name = loc_id  # location_id used as display label in per-store printout
    mae  = mean_absolute_error(grp['net_sales'], grp['predicted'])
    mape = mean_absolute_percentage_error(grp['net_sales'], grp['predicted'])
    store_results.append({'location_id': loc_id, 'mae': mae, 'mape': mape, 'n': len(grp)})
    print(f'  {loc_id:<15} {mae:>8,.0f} {mape*100:>7.1f}% {len(grp):>6}')

# Band accuracy
BANDS = [
    ('<25k',    0,     25000),
    ('25k-30k', 25000, 30000),
    ('30k-35k', 30000, 35000),
    ('35k-40k', 35000, 40000),
    ('40k-45k', 40000, 45000),
    ('45k-50k', 45000, 50000),
    ('50k+',    50000, 9999999),
]

def weekly_band(weekly_sales):
    for name, lo, hi in BANDS:
        if lo <= weekly_sales < hi:
            return name
    return '50k+'

print('\n=== WEEKLY BAND ACCURACY (last 8 weeks) ===')
# Aggregate to weekly
val_df['week'] = val_df['sale_date'].dt.to_period('W')
weekly_val = val_df.groupby(['location_id', 'week']).agg(
    actual_weekly=('net_sales', 'sum'),
    predicted_weekly=('predicted', 'sum')
).reset_index()
weekly_val = weekly_val[weekly_val['actual_weekly'] > 0]
weekly_val['actual_band']    = weekly_val['actual_weekly'].apply(weekly_band)
weekly_val['predicted_band'] = weekly_val['predicted_weekly'].apply(weekly_band)
weekly_val['band_hit'] = weekly_val['actual_band'] == weekly_val['predicted_band']
band_accuracy = weekly_val['band_hit'].mean()
print(f'  Band accuracy: {band_accuracy*100:.1f}% ({weekly_val["band_hit"].sum()}/{len(weekly_val)} weeks correct)')

# Feature importance
print('\n=== TOP 20 FEATURE IMPORTANCES ===')
fi = pd.DataFrame({'feature': model.feature_name(), 'importance': model.feature_importance(importance_type='gain')})
fi = fi.sort_values('importance', ascending=False).head(20)
for _, row in fi.iterrows():
    print(f'  {row["feature"]:<35} {row["importance"]:>10,.0f}')

# Save model
model_path = 'C:/Users/BretElliott/ramz-accounting/labor/lgbm_model_v1.pkl'
with open(model_path, 'wb') as f:
    pickle.dump(model, f)

# Save metadata
meta = {
    'model_version': 'v1',
    'feature_cols': feature_cols,
    'cat_features': cat_features,
    'best_iteration': model.best_iteration,
    'overall_mae': round(overall_mae, 2),
    'overall_mape': round(overall_mape * 100, 2),
    'band_accuracy': round(band_accuracy * 100, 2),
    'train_rows': len(train_df),
    'val_rows': len(val_df),
    'stores': df['location_id'].nunique(),
}
with open('C:/Users/BretElliott/ramz-accounting/labor/model_meta_v1.json', 'w') as f:
    json.dump(meta, f, indent=2)

print(f'\nModel saved to {model_path}')
print(f'Metadata saved to model_meta_v1.json')
print(f'\nSummary: MAE=${overall_mae:,.0f} | MAPE={overall_mape*100:.1f}% | Band accuracy={band_accuracy*100:.1f}%')
