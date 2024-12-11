import pandas as pd


def load_csv(filepath):
    """Load data from a CSV file"""
    return pd.read_csv(filepath)


def load_from_database(connection_string, query):
    """Load data from a SQL database"""
    import sqlalchemy
    engine = sqlalchemy.create_engine(connection_string)
    return pd.read_sql(query, engine)
