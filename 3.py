from sqlalchemy import create_engine,Column,String,Integer,Float,ForeignKey
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
engine = create_engine('sqlite:///Ra.db', echo = False)
session=sessionmaker(bind=engine)
session=session()
Base=declarative_base()
import csv
class Rate_puf(Base):
    __tablename__ = 'rate_puf'
    BusinessYear=Column(Integer)
    rateid=Column(Integer, primary_key=True,autoincrement=True)
    StateCode=Column(String(4))
    IssuerId=Column(Integer)
    SourceName=Column(String(10000))
    VersionNum=Column(Integer)
    ImportDate=Column(String(10000))
    IssuerId2=Column(Integer)
    FederalTIN=Column(String(10000))
    RateEffectiveDate=Column(String(10000))
    RateExpirationDate=Column(String(10000))
    PlanId=Column(Integer)
    RatingAreaId=Column(String(10000))
    Tobacco=Column(String(10000))
    Age=Column(String(10000))
    IndividualRate=Column(Float(10))
    IndividualTobaccoRate=Column(Float(10))
    Couple=Column(Float(10))
    PrimarySubscriberAndOneDependent=Column(Float(10))
    PrimarySubscriberAndTwoDependents=Column(Float(10))
    PrimarySubscriberAndThreeOrMoreDependents=Column(Float(10))
    CoupleAndOneDependent=Column(Float(10))
    CoupleAndTwoDependents=Column(Float(10))
    CoupleAndThreeOrMoreDependents=Column(Float(10))
    RowNumber=Column(Integer)
Base.metadata.create_all(engine)
year=str(input('Enter a year to load(2014,2015,2016)'))
path=r'C:\Users\DELL\PycharmProjects\pythonProject\input'+year+'\Rate_PUF.csv'
with open(path, 'r', encoding='utf-8')as f:
    Dictreader = csv.DictReader(f)
    y=0
    for row in Dictreader:
        s1 = Rate_puf(**row)
        session.add(s1)
        y=y+1
        print(y)

    session.commit()
print("sucessfully created the Database")
class Audit_table(Base):
    __tablename__ = 'Audit_table'
    audit_id = Column(Integer, primary_key=True, autoincrement=True)  # Indicates the no of changes done in my table
    BusinessYear = Column(Integer, ForeignKey('rate_puf'))
    start_time = Column(String(1000))
    end_time = Column(String(1000))
    no_of_records = Column(Integer)
    transaction_status = Column(String(1000))
Base.metadata.create_all(engine)
confirm = input("Do you want to transform load:{yes/no)\n")
if (confirm == "yes"):
    start = datetime.now()
    # Checking Dataset is already available
    check = session.query().filter_by(BusinessYear=year).first()
    if check is not None:
        session.delete(check)
        print("Old Records Deleted")
    flag = 0
    checkpoint = 0
    total_records = 0
    try:
        mod_year = str(input("Enter the year which you want to add 2016,2015,2014"))
        path = r'C:\Users\DELL\PycharmProjects\pythonProject\input' + mod_year + '\Rate_PUF.csv'
        with open(path, 'r', encoding='utf-8')as f:
            Dictreader = csv.DictReader(f)
            for row in Dictreader:
                try:
                    record = Rate_puf(**row)
                    session.add(record)
                    print(flag, "th record")
                    flag += 1

                except KeyboardInterrupt as k:
                    Rate_puf.__table__.drop(engine)
                    print("Process aborted")
                    break
                except MemoryError as e:
                    session.rollback()
                    Rate_puf.__table__.drop(engine)
                    session.add(Audit_table(Businessyear=year, start_time=start, end_time=datetime.now(),
                                            no_of_records=total_records, transaction_status="Failure"))
                    session.commit()
                    print("Transaction roll back")
                    break
            else:
                total_records += flag
                session.commit()
                session.add(Audit_table(Businessyear=year, start_time=start, end_time=datetime.now(),
                                        no_of_records=total_records, transaction_status="Success"))
                session.commit()
                print("All records are inserted:Transaction completed")

    except UnicodeDecodeError as e:
        Rate_puf.__table__.drop(engine)
        print("Check encoding")
    except FileNotFoundError as e:
        print("Dataset for Year doesnt exist")