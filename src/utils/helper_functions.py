def save_to_csv(df, filepath):
    """Save dataframe to a CSV file"""
    df.to_csv(filepath, index=False)


def load_from_json(filepath):
    """Load data from a JSON file"""
    import json
    with open(filepath, 'r') as f:
        data = json.load(f)
    return data
