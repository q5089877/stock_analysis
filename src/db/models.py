# models.py: Define ORM models (e.g., StockDaily, Indicator, SectorSummary)
F

Base = declarative_base()

class StockDaily(Base):
    __tablename__ = 'stock_daily'
    # define columns here

class Indicator(Base):
    __tablename__ = 'indicator'
    # define columns here

class SectorSummary(Base):
    __tablename__ = 'sector_summary'
    # define columns here
