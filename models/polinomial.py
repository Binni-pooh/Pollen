import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, r2_score, root_mean_squared_error, mean_squared_error
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import PolynomialFeatures, StandardScaler

from models.training_data import get_training_data

# 1. Load data
features_train, features_test, target_train, target_test, meteo_features, target_col = get_training_data()

# 2. Create a ColumnTransformer for selective Polynomial Features
preprocessor = ColumnTransformer(
    transformers=[
        ('poly', PolynomialFeatures(degree=2, include_bias=False), meteo_features)
    ],
    remainder='passthrough'
)

# 3. Create the final ML Pipeline
model = Pipeline(steps=[
    ('preprocessor', preprocessor),
    ('scaler', StandardScaler()),
    ('regressor', LinearRegression())
])

# 4. Train the model
model.fit(features_train, target_train)
print("--- Model ---")
print("Polynomial Regression")
print("PolynomialFeatures degree: 2")
print("Scaler: StandardScaler")
print()

# 5. Evaluate metrics
target_pred = model.predict(features_test)

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