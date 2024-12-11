from src.data.load_data import load_csv
from src.data.preprocess import clean_data
from src.features.build_features import add_features
from src.models.train_model import train_model
from src.models.evaluate_model import evaluate_model
from src.visualization.visualize import plot_data


def main():
    # Load data
    df = load_csv('data/raw/data.csv')

    # Preprocess data
    df = clean_data(df)

    # Add features
    df = add_features(df)

    # Split data
    from sklearn.model_selection import train_test_split
    X = df.drop('target', axis=1)
    y = df['target']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Train model
    model = train_model(X_train, y_train)

    # Evaluate model
    evaluate_model(model, X_test, y_test)

    # Visualize data
    plot_data(df, ['feature1', 'feature2'])


if __name__ == "__main__":
    main()
