def clean_data(df):
    """Clean the data by removing duplicates and handling missing values"""
    df = df.drop_duplicates()
    df = df.fillna(method='ffill')
    return df


def normalize_data(df, columns):
    """Normalize specified columns in the dataframe"""
    from sklearn.preprocessing import MinMaxScaler
    scaler = MinMaxScaler()
    df[columns] = scaler.fit_transform(df[columns])
    return df
