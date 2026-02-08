from sqlalchemy import create_engine

DB_USER = "postgres"
DB_PASSWORD = "Admin123"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "elt_warehouse"

DB_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

def get_engine():
    """
    Returns a SQLAlchemy engine for Postgres warehouse
    """
    return create_engine(DB_URL)
