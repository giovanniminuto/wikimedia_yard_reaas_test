from sklearn.model_selection import train_test_split

from wikimedia_yard_reaas_test.utils import create_spark, read_delta_table
from wikimedia_yard_reaas_test.train_and_evaluate_functions import (
    build_pipeline,
    build_preprocessor,
    hyperparameter_search,
    evaluate_model,
    feature_importance,
)


# ----------------------------
# Main
# ----------------------------


spark = create_spark()

# Paths
gold_path_train_set = "data/train_set/pageviews/2025-01"
gold_path_test_set = "data/test_set/pageviews/2025-01"

# Load
df_train = read_delta_table(spark, gold_path_train_set)
df_test_set = read_delta_table(spark, gold_path_test_set)

# Train features/target
y = df_train["churn"].astype(int)
X = df_train.drop(columns=["churn"])

categorical_cols = [
    "page_title",
    "language",
    "database_name",
    "namespace",
    "is_mobile",
    "sparsity_level",
]
numeric_cols = [c for c in X.columns if c not in categorical_cols]

preprocessor = build_preprocessor(categorical_cols, numeric_cols)
pipe = build_pipeline(preprocessor)

# Split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, stratify=y, random_state=42
)

# Search best model
best_model = hyperparameter_search(pipe, X_train, y_train)

# Evaluate
X_sample = X_test.sample(n=1000, random_state=42)
y_sample = y_test.loc[X_sample.index]
evaluate_model(best_model, X_sample, y_sample, label="Train-Test split")

# Second test set
y_set = df_test_set["churn"].astype(int)
X_set = df_test_set.drop(columns=["churn"])

# NOTE: we reuse the best_model from training search
X_train_set, X_test_set, y_train_set, y_test_set = train_test_split(
    X_set, y_set, test_size=0.2, stratify=y_set, random_state=42
)
X_sample_set = X_test_set.sample(n=1000, random_state=42)
y_sample_set = y_test_set.loc[X_sample.index]
evaluate_model(best_model, X_sample_set, y_sample_set, label="External test set")

# Feature importance
feature_importance(best_model, X_test, y_test)
