import joblib
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score, root_mean_squared_error, mean_squared_error
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from models.training_data import get_training_data

# 1. Load data
features_train, features_test, target_train, target_test, meteo_features, target_col = get_training_data()

# 2. Create Pipeline
n_estimators = 200
rf_model = make_pipeline(
    StandardScaler(),
    RandomForestRegressor(n_estimators=n_estimators, random_state=42, n_jobs=-1)
)

# 3. Train the model
rf_model.fit(features_train, target_train)
print("--- Model ---")
print("Random forest")
print(f"Number of estimators: {n_estimators}")
print("Scaler: StandardScaler")
print()

# 4. Make predictions on the test set
target_pred = rf_model.predict(features_test)

# 5. Evaluate model performance
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

importances = rf_model.named_steps['randomforestregressor'].feature_importances_
feature_importance_df = pd.DataFrame({'Feature': meteo_features, 'Importance': importances}).sort_values(by='Importance', ascending=False)

print("Top 10 features:")
print(feature_importance_df)
print()