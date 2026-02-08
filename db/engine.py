
from sqlalchemy import create_engine

DB_URL = "postgresql+psycopg2://postgres:Admin123@localhost:5432/elt_warehouse"

engine = create_engine(DB_URL)
