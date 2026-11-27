import pandas as pd
from lightgbm import LGBMRegressor, early_stopping
from sklearn.metrics import mean_absolute_error, r2_score, mean_squared_error, root_mean_squared_error

from models.training_data import get_training_data

# --- 1. DATA LOADING ---
features_train, features_test, target_train, target_test, meteo_features, target_col = get_training_data()

# --- 2. MODEL TRAINING ---
lgbm_model = LGBMRegressor(
    n_estimators=3000,
    learning_rate=0.005,
    num_leaves=31,
    max_depth=-1,
    min_child_samples=30,
    subsample=0.8,
    colsample_bytree=0.8,
    reg_alpha=0.2,
    reg_lambda=0.2,
    random_state=42,
    n_jobs=-1
)

# Fit the model
lgbm_model.fit(
    features_train,
    target_train,
    eval_X=features_test,
    eval_y=target_test,
    callbacks=[early_stopping(stopping_rounds=50)]
)

print("--- Model ---")
print("LightGBM")
print(f"Number of estimators: 3000")
print()

# --- 3. PREDICTIONS ---
target_pred = lgbm_model.predict(features_test)

# --- 4. EVALUATION ---
mae = mean_absolute_error(target_test, target_pred)
mse = mean_squared_error(target_test, target_pred)
rmse = root_mean_squared_error(target_test, target_pred)
r2 = r2_score(target_test, target_pred)

print("--- Evaluation Results ---")
print(f"MAE (Mean Absolute Error): {mae:.3f} No/m³")
print(f"MSE (Mean Squared Error): {mse:.3f} (No/m³)^2")
print(f"RMSE (Root Mean Squared Error): {rmse:.3f} No/m³")
print(f"R2 (Coefficient of Determination): {r2:.3f}")
print()

# --- 5. FEATURE IMPORTANCE ---
# Extract feature importances from the trained model
importance = lgbm_model.feature_importances_
imp_df = pd.DataFrame({
    "feature": features_train.columns,
    "importance": importance
}).sort_values("importance", ascending=False)

print("Top 10 features:")
print(imp_df.head(10))