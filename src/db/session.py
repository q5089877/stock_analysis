# session.py: Create SQLAlchemy engine and session
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def get_session(db_url):
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    return Session()
