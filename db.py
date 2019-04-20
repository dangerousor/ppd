# coding: utf-8
from sqlalchemy import CHAR, Column, DateTime, Float, JSON, String, Text, text, create_engine
from sqlalchemy.dialects.mysql import INTEGER
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import exists

Base = declarative_base()
metadata = Base.metadata


class DebtRecord(Base):
    __tablename__ = 'debt_record'

    index = Column(INTEGER(11), primary_key=True, autoincrement=True)
    lender_id = Column(INTEGER(11))
    lender_name = Column(Text)
    owing_principal = Column(Float)
    price_for_sell = Column(Float)
    buy_source_type = Column(Text)
    close_bid_date = Column(DateTime)
    debt_deal_id = Column(Text)
    buyer_user_name = Column(Text)


class Loan(Base):
    __tablename__ = 'loan'

    loan_id = Column(INTEGER(11), primary_key=True, unique=True)
    title = Column(Text)
    stamps = Column(Text)
    credit = Column(Text)
    user_name = Column(Text)
    amount = Column(INTEGER(11))
    current_rate = Column(Float)
    months = Column(INTEGER(11))
    repayment_method = Column(Text)
    interest_date = Column(CHAR(8), server_default=text("借款开始日"))
    progress = Column(INTEGER(11))
    bid_users = Column(INTEGER(11))
    list_end_date = Column(DateTime)
    bid_status = Column(Text)
    loan_use = Column(Text)
    user_id = Column(INTEGER(11))


class PreviousListing(Base):
    __tablename__ = 'previous_listings'

    index = Column(INTEGER(11), primary_key=True, autoincrement=True)
    user_id = Column(INTEGER(11))
    title = Column(Text)
    rate = Column(Float)
    months = Column(INTEGER(11))
    amount = Column(Float)
    creation_date = Column(DateTime)
    status = Column(Text)
    loan_id = Column(INTEGER(11))


class Record(Base):
    __tablename__ = 'record'

    index = Column(INTEGER(11), primary_key=True, autoincrement=True)
    loan_id = Column(INTEGER(11))
    lender_id = Column(INTEGER(11))
    lender_name = Column(Text)
    source = Column(Text)
    rate = Column(Float)
    participation_amount = Column(Float)
    creation_date = Column(DateTime)


class Statistic(Base):
    __tablename__ = 'statistics'

    loan_id = Column(INTEGER(11), primary_key=True, unique=True)
    success_borrow_num = Column(INTEGER(11))
    first_success_borrow_date = Column(DateTime)
    history = Column(Text)
    success_pay_num = Column(INTEGER(11))
    normal_num = Column(INTEGER(11))
    overdue_less_num = Column(INTEGER(11))
    overdue_more_num = Column(INTEGER(11))
    owing_amount_map = Column(JSON)
    overdue_day_map = Column(JSON)
    total_principal = Column(Float)
    owing_amount = Column(Float)
    loan_amount_max = Column(Float)
    debt_amount_max = Column(Float)
    debt_amount_map = Column(JSON)
    business_and_financial_info = Column(Text)
    repayment_power_change = Column(Text)
    bad_info = Column(Text)
    administrative_penalty = Column(JSON)


class User(Base):
    __tablename__ = 'user'

    user_id = Column(INTEGER(11), primary_key=True, unique=True)
    real_name = Column(Text)
    id_number = Column(String(24))
    gender = Column(String(8))
    age = Column(String(8))
    register_date = Column(DateTime)
    degree = Column(Text)
    graduation = Column(Text)
    study_style = Column(Text)
    overdue_status = Column(String(8))
    overdue_types = Column(Text)
    repayment_source = Column(Text)
    work_info = Column(Text)
    income = Column(Text)
    bal_amount = Column(Text)
    industry = Column(Text)
    auths = Column(Text)


class DBWorker:
    engine = create_engine("mysql+pymysql://%s:%s:%s/%s" % ("root", "Lh201903@rm-uf6e1v96s9bc8yvjd0o.mysql.rds.aliyuncs.com", "3306", "test"))
    DBsession = sessionmaker(bind=engine)

    def get_session(self):
        return self.DBsession()

    def insert(self, table):
        session = self.get_session()
        session.add(table)
        session.commit()
        session.close()

    def insert_all(self, tables):
        session = self.get_session()
        session.add_all(tables)
        session.commit()
        session.close()

    def search(self, params):
        session = self.get_session()
        res = session.query(exists().where(params)).scalar()
        session.close()
        return res
