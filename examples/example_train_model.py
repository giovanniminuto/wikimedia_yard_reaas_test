# ---------- Spark ML: GBT + Cross-Validation + Metrics ----------
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator

from wikimedia_yard_reaas_test.utils import create_spark, read_delta_table


# =========================
# 0) Train / Test split
# =========================
delta_supervised_path = "data/supervised"

spark = create_spark()
delta_supervised = read_delta_table(spark, delta_supervised_path).drop("language")
delta_supervised.show(1000)

train_df, test_df = delta_supervised.randomSplit([0.8, 0.2], seed=42)

train_df = train_df.withColumn("is_mobile_int", F.col("is_mobile").cast("int"))
test_df = test_df.withColumn("is_mobile_int", F.col("is_mobile").cast("int"))


# =========================
# 1) (Optional) Class weights
# =========================
# Compute churn prevalence and create inverse-frequency weights to help PR-AUC under imbalance.
label_stats = train_df.groupBy("churn").count().withColumnRenamed("count", "cnt").cache()
n_pos = (
    label_stats.filter("churn = 1").select("cnt").first()[0]
    if label_stats.filter("churn = 1").count()
    else 0
)
n_neg = (
    label_stats.filter("churn = 0").select("cnt").first()[0]
    if label_stats.filter("churn = 0").count()
    else 0
)
total = n_pos + n_neg if (n_pos is not None and n_neg is not None) else 1

# Avoid division by zero
w_pos = float(total) / (2.0 * n_pos) if n_pos > 0 else 1.0
w_neg = float(total) / (2.0 * n_neg) if n_neg > 0 else 1.0

train_df = train_df.withColumn(
    "weight", F.when(F.col("churn") == 1, F.lit(w_pos)).otherwise(F.lit(w_neg))
)

train_df = train_df.withColumn(
    "page_bucket",
    F.when(F.col("page_bucket").isNull() | (F.col("page_bucket") == ""), "Other").otherwise(
        F.col("page_bucket")
    ),
)

test_df = test_df.withColumn(
    "page_bucket",
    F.when(F.col("page_bucket").isNull() | (F.col("page_bucket") == ""), "Other").otherwise(
        F.col("page_bucket")
    ),
)

train_df = train_df.withColumn(
    "database_name",
    F.when(
        F.col("database_name").isNull() | (F.col("database_name") == ""), "unknown_db"
    ).otherwise(F.col("database_name")),
)

test_df = test_df.withColumn(
    "database_name",
    F.when(
        F.col("database_name").isNull() | (F.col("database_name") == ""), "unknown_db"
    ).otherwise(F.col("database_name")),
)

# =========================
# 2) Feature engineering
# =========================
# Categorical columns — index + one-hot.
cat_cols = ["database_name", "page_bucket"]  # removed language
indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in cat_cols]
encoders = OneHotEncoder(
    inputCols=[f"{c}_idx" for c in cat_cols],
    outputCols=[f"{c}_ohe" for c in cat_cols],
    handleInvalid="keep",
)

# Numeric feature columns from your anti-seasonality pipeline
num_cols = [
    "active_days",
    "avg_norm",
    "std_norm",
    "max_norm",
    "views_last1",
    "views_last3",
    "views_last7",
    "is_mobile_int",
]

assembler = VectorAssembler(
    inputCols=num_cols + [f"{c}_ohe" for c in cat_cols], outputCol="features", handleInvalid="keep"
)

# =========================
# 3) Classifier (GBT)
# =========================
# Why GBT: strong on tabular data, non-linearities, interactions, no scaling needed, robust to skew.
gbt = GBTClassifier(
    labelCol="churn",
    featuresCol="features",
    weightCol="weight",  # remove if you don't want class weighting
    maxIter=100,
    maxDepth=5,
    stepSize=0.1,
    seed=42,
)

pipeline = Pipeline(stages=[*indexers, encoders, assembler, gbt])

# =========================
# 4) Hyper-parameter grid + CV
# =========================
paramGrid = (
    ParamGridBuilder()
    .addGrid(gbt.maxDepth, [3, 5, 7])
    .addGrid(gbt.maxIter, [60, 100, 140])
    .addGrid(gbt.stepSize, [0.05, 0.1])
    .build()
)

# Primary evaluator: ROC-AUC; we'll also compute PR-AUC after
roc_eval = BinaryClassificationEvaluator(
    labelCol="churn", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
)

cv = CrossValidator(
    estimator=pipeline,
    estimatorParamMaps=paramGrid,
    evaluator=roc_eval,
    numFolds=3,  # increase on cluster if time allows
    parallelism=2,  # increase if your environment can parallelize folds
    seed=42,
)

# =========================
# 5) Fit & Evaluate
# =========================
cv_model = cv.fit(train_df)

pred_test = cv_model.transform(test_df)

# Metrics
roc_auc = roc_eval.evaluate(pred_test)
pr_eval = BinaryClassificationEvaluator(
    labelCol="churn", rawPredictionCol="rawPrediction", metricName="areaUnderPR"
)
pr_auc = pr_eval.evaluate(pred_test)

print(f"[TEST] ROC-AUC = {roc_auc:.4f}")
print(f"[TEST] PR-AUC  = {pr_auc:.4f}")

# =========================
# 6) Best params + quick diagnostics
# =========================
# Best model’s GBT stage is last in the pipeline
best_pipeline_model = cv_model.bestModel
best_gbt = best_pipeline_model.stages[-1]
print(
    "Best GBT Params:",
    f"maxDepth={best_gbt.getOrDefault('maxDepth')},",
    f"maxIter={best_gbt.getOrDefault('maxIter')},",
    f"stepSize={best_gbt.getOrDefault('stepSize')}",
)

# Confusion matrix at default 0.5 threshold (Spark’s default threshold for GBT)
pred_test.select("churn", (F.col("probability")[1] > 0.5).cast("int").alias("pred")).groupBy(
    "churn", "pred"
).count().show()

# If you want to choose a better threshold for PR (optional):
# - collect probabilities & labels, sweep thresholds, maximize F1 or expected cost.
