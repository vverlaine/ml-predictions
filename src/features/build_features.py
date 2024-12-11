def add_features(df):
    """Add new features to the dataframe"""
    df['feature1'] = df['column1'] * df['column2']
    df['feature2'] = df['column3'].apply(lambda x: x**2)
    return df
