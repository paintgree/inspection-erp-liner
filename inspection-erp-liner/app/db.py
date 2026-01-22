from sqlmodel import SQLModel, create_engine, Session

# SQLite in Render filesystem (good for prototype)
DATABASE_URL = "sqlite:///./data.db"
engine = create_engine(DATABASE_URL, echo=False, connect_args={"check_same_thread": False})

def create_db_and_tables():
    SQLModel.metadata.create_all(engine)

def get_session():
    with Session(engine) as session:
        yield session
