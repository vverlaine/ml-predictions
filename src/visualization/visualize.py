import matplotlib.pyplot as plt
import numpy as np


def plot_data(df, columns):
    """Plot data from the dataframe"""
    df[columns].plot(kind='line')
    plt.show()


def plot_feature_importance(model, feature_names):
    """Plot feature importance of a trained model"""
    importances = model.feature_importances_
    indices = np.argsort(importances)[::-1]

    plt.figure()
    plt.title("Feature importances")
    plt.bar(range(len(indices)), importances[indices], color="r", align="center")
    plt.xticks(range(len(indices)), [feature_names[i] for i in indices], rotation=90)
    plt.xlim([-1, len(indices)])
    plt.show()
