from sklearn.model_selection import RandomizedSearchCV
from sklearn.preprocessing import OneHotEncoder, StandardScaler, FunctionTransformer
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import roc_auc_score, average_precision_score, classification_report
from sklearn.inspection import permutation_importance

from typing import List, Union

import numpy as np
import pandas as pd


def build_preprocessor(categorical_cols: List[str], numeric_cols: List[str]) -> ColumnTransformer:
    """Build preprocessing pipeline with OneHot + StandardScaler.

    Args:
        categorical_cols (List[str]): Names of categorical columns.
        numeric_cols (List[str]): Names of numeric columns.

    Returns:
        ColumnTransformer: Transformer applying OneHotEncoder to categorical columns
        and StandardScaler to numeric columns.
    """
    categorical_transformer = OneHotEncoder(handle_unknown="ignore")
    numeric_transformer = StandardScaler()
    return ColumnTransformer(
        transformers=[
            ("cat", categorical_transformer, categorical_cols),
            ("num", numeric_transformer, numeric_cols),
        ]
    )


def build_pipeline(preprocessor: ColumnTransformer) -> Pipeline:
    """Build full pipeline with preprocessing and classifier.

    Args:
        preprocessor (ColumnTransformer): Preprocessing transformer.

    Returns:
        Pipeline: Pipeline including preprocessing, dense conversion, and classifier.
    """
    clf = HistGradientBoostingClassifier(random_state=42)

    def to_dense(x: Union[np.ndarray, pd.DataFrame]) -> np.ndarray:
        return x.toarray() if hasattr(x, "toarray") else x

    pipe = Pipeline(
        steps=[
            ("preprocess", preprocessor),
            ("to_dense", FunctionTransformer(to_dense)),
            ("clf", clf),
        ]
    )
    return pipe


def hyperparameter_search(pipe: Pipeline, X_train: pd.DataFrame, y_train: pd.Series) -> Pipeline:
    """Run RandomizedSearchCV on the pipeline.

    Args:
        pipe (Pipeline): Model pipeline to optimize.
        X_train (pd.DataFrame): Training features.
        y_train (pd.Series): Training labels.

    Returns:
        Pipeline: Best estimator pipeline after hyperparameter search.
    """
    param_dist = {
        "clf__max_depth": [3, 5, 7, None],
        "clf__learning_rate": [0.01, 0.05, 0.1, 0.2],
        "clf__max_iter": [100, 200, 500],
        "clf__min_samples_leaf": [10, 20, 50],
        "clf__l2_regularization": [0.0, 1.0, 5.0],
    }

    search = RandomizedSearchCV(
        pipe,
        param_distributions=param_dist,
        n_iter=2,
        scoring="roc_auc",
        cv=3,
        verbose=2,
        n_jobs=-1,
        random_state=42,
    )

    # use a sample for speed
    X_small = X_train.sample(n=10000, random_state=42)
    y_small = y_train.loc[X_small.index]

    search.fit(X_small, y_small)
    print("Best ROC-AUC (CV):", search.best_score_)
    print("Best params:", search.best_params_)
    return search.best_estimator_


def evaluate_model(
    model: Pipeline, X_test: pd.DataFrame, y_test: pd.Series, label: str = "Test"
) -> None:
    """Evaluate model with ROC-AUC, PR-AUC and classification report.

    Args:
        model (Pipeline): Trained pipeline.
        X_test (pd.DataFrame): Test features.
        y_test (pd.Series): Test labels.
        label (str, optional): Label for printing results. Defaults to "Test".
    """

    y_pred_proba = model.predict_proba(X_test)[:, 1]
    y_pred = model.predict(X_test)

    print(f"{label} ROC-AUC:", roc_auc_score(y_test, y_pred_proba))
    print(f"{label} PR-AUC:", average_precision_score(y_test, y_pred))
    print(classification_report(y_test, y_pred))


def feature_importance(model: Pipeline, X: pd.DataFrame, y: pd.Series) -> None:
    """Compute permutation feature importance.

    Args:
        model (Pipeline): Trained pipeline.
        X (pd.DataFrame): Feature data.
        y (pd.Series): Labels.
    """
    r = permutation_importance(model, X, y, n_repeats=10, random_state=42, n_jobs=-1)
    sorted_idx = r.importances_mean.argsort()[::-1]

    for i in sorted_idx[:10]:
        print(f"{X.columns[i]}: {r.importances_mean[i]:.4f}")
