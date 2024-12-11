from sklearn.ensemble import RandomForestClassifier


def train_model(X_train, y_train):
    """Train a RandomForest model"""
    model = RandomForestClassifier(n_estimators=100)
    model.fit(X_train, y_train)
    return model
