from sqlmodel import SQLModel, create_engine, Session
import os

DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL:
    # Postgres (Neon)
    engine = create_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
else:
    # SQLite fallback (local)
    DB_PATH = os.getenv("INSPECTION_DB", "inspection.db")
    engine = create_engine(
        f"sqlite:///{DB_PATH}",
        echo=False,
        connect_args={"check_same_thread": False},
    )

def create_db_and_tables():
    SQLModel.metadata.create_all(engine)

def get_session():
    with Session(engine) as session:
        yield session
