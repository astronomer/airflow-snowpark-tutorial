"""Test several classification models on the ski dataset."""

import pandas as pd
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import roc_curve, auc, ConfusionMatrixDisplay
from sklearn.preprocessing import label_binarize


def plot_metrics(y_pred, y_test, pipeline, name):
    fig, ax = plt.subplots(1, 2, figsize=(15, 6))

    ConfusionMatrixDisplay.from_predictions(y_test, y_pred, ax=ax[0], cmap="Blues")
    ax[0].set_title(f"Confusion Matrix - {name}")

    classes = pipeline.named_steps["classifier"].classes_
    y_test_bin = label_binarize(y_test, classes=classes)
    y_score = pipeline.predict_proba(X_test)

    assert list(pipeline.named_steps["classifier"].classes_) == list(
        classes
    ), "Class order mismatch!"

    fpr = dict()
    tpr = dict()
    roc_auc = dict()

    for i, cls in enumerate(classes):
        fpr[cls], tpr[cls], _ = roc_curve(y_test_bin[:, i], y_score[:, i])
        roc_auc[cls] = auc(fpr[cls], tpr[cls])

        ax[1].plot(
            fpr[cls], tpr[cls], label=f"ROC curve (area = {roc_auc[cls]:.2f}) for {cls}"
        )

    ax[1].plot([0, 1], [0, 1], "k--")
    ax[1].set_xlim([0.0, 1.0])
    ax[1].set_ylim([0.0, 1.05])
    ax[1].set_xlabel("False Positive Rate")
    ax[1].set_ylabel("True Positive Rate")
    ax[1].set_title(f"ROC Curve - {name}")
    ax[1].legend(loc="lower right")

    plt.tight_layout()
    plt.savefig(f"{name}_metrics.png")


df = pd.read_csv("ski_dataset.csv")

X = df.drop(columns=["afternoon_beverage", "skier_id"])
y = df["afternoon_beverage"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

categorical_features = ["resort", "ski_color", "jacket_color", "had_lunch"]
numeric_features = ["hours_skied", "snow_quality", "cm_of_new_snow"]
preprocessor = ColumnTransformer(
    transformers=[
        ("num", StandardScaler(), numeric_features),
        ("cat", OneHotEncoder(drop="first"), categorical_features),
    ]
)

classifiers = [
    ("Logistic Regression", LogisticRegression(max_iter=10000)),
    (
        "Random Forest",
        RandomForestClassifier(random_state=42, max_depth=5, max_leaf_nodes=20),
    ),
    (
        "Decision Tree",
        DecisionTreeClassifier(random_state=42, max_depth=5, max_leaf_nodes=20),
    ),
    ("K-Nearest Neighbors", KNeighborsClassifier(n_neighbors=2)),
]

for name, clf in classifiers:
    pipeline = Pipeline([("preprocessor", preprocessor), ("classifier", clf)])
    pipeline.fit(X_train, y_train)
    score = pipeline.score(X_test, y_test)
    print(f"{name} Accuracy: {score:.4f}")
    y_pred = pipeline.predict(X_test)

    plot_metrics(y_pred, y_test, pipeline, name)
