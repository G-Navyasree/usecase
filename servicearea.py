from sqlalchemy import create_engine,Column,String,Integer,ForeignKey
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
engine = create_engine('sqlite:///serv.db', echo = False)
session=sessionmaker(bind=engine)
session=session()
Base=declarative_base()
import csv
class Service_area(Base):
    __tablename__ = 'Service_area'
    service_id = Column(Integer, primary_key=True, autoincrement=True)
    BusinessYear = Column(Integer)
    StateCode = Column(String(50))
    IssuerId = Column(Integer)
    SourceName = Column(String(50))
    VersionNum = Column(Integer)
    ImportDate = Column(String(50))
    IssuerId2 = Column(Integer)
    ServiceAreaId = Column(String(50))
    ServiceAreaName = Column(String(50))
    CoverEntireState = Column(String(4))
    County = Column(Integer)
    PartialCounty = Column(String(4))
    ZipCodes = Column(String(10000000000000000000000000000))
    PartialCountyJustification = Column(String(10000000000000000000000000000))
    RowNumber = Column(Integer)
    MarketCoverage = Column(String(50))
    DentalOnly=Column(String(10))
Base.metadata.create_all(engine)
year = int( input("Enter a year to load into the database"))
if (year==int(2014)):
    with open(r'C:\Users\DELL\PycharmProjects\pythonProject\input\2014\Service_Area_PUF2014.csv','r',encoding='utf-8') as f:
        Dictreader = csv.DictReader(f)
        for row in Dictreader:
            x=dict(row)
            s1= Service_area (BusinessYear=x["BusinessYear"],StateCode =x["StateCode"],IssuerId=x["IssuerId"],SourceName=x["SourceName"],VersionNum=x["VersionNum"],IssuerId2=x["IssuerId2"],ServiceAreaId =x["ServiceAreaId"],
                ServiceAreaName=x["ServiceAreaName"],CoverEntireState=x["CoverEntireState"],County=x["County"],PartialCounty=x["PartialCounty"],
                ZipCodes=x["ZipCodes"], PartialCountyJustification=x["PartialCountyJustification"],RowNumber=x["RowNumber"],
                DentalOnly =x["DentalOnly"],MarketCoverage =x["MarketCoverage"],ImportDate= x["ImportDate"])
            session.add(s1)
        session.commit()
elif (year==int(2015)):
    with open(r'C:\Users\DELL\PycharmProjects\pythonProject\input\2015\Service_Area_PUF2015.csv',
              'r',encoding='utf-8') as f:
        Dictreader = csv.DictReader(f)
        for row in Dictreader:
            x = dict(row)
            s1 = Service_area(BusinessYear=x["BusinessYear"],StateCode =x["StateCode"],IssuerId=x["IssuerId"],SourceName=x["SourceName"],VersionNum=x["VersionNum"],IssuerId2=x["IssuerId2"],ServiceAreaId =x["ServiceAreaId"],
                ServiceAreaName=x["ServiceAreaName"],CoverEntireState=x["CoverEntireState"],County=x["County"],PartialCounty=x["PartialCounty"],
                ZipCodes=x["ZipCodes"], PartialCountyJustification=x["PartialCountyJustification"],RowNumber=x["RowNumber"],
                              DentalOnly =x["DentalOnly"],MarketCoverage =x["MarketCoverage"],ImportDate= x["ImportDate"])
            session.add(s1)
        session.commit()

else:
    with open(r'C:\Users\DELL\PycharmProjects\pythonProject\input\2016\Service_Area_PUF2016.csv',
             'r', encoding='utf-8') as f:
        Dictreader = csv.DictReader(f)
        for row in Dictreader:
            x = dict(row)
            s1 = Service_area(BusinessYear=x["BusinessYear"],StateCode =x["StateCode"],IssuerId=x["IssuerId"],SourceName=x["SourceName"],VersionNum=x["VersionNum"],IssuerId2=x["IssuerId2"],ServiceAreaId =x["ServiceAreaId"],
                              ServiceAreaName=x["ServiceAreaName"],CoverEntireState=x["CoverEntireState"],County=x["County"],PartialCounty=x["PartialCounty"],
                              ZipCodes=x["ZipCodes"], PartialCountyJustification=x["PartialCountyJustification"],RowNumber=x["RowNumber"],
                              DentalOnly =x["DentalOnly"],MarketCoverage =x["MarketCoverage"],ImportDate= x["ImportDate"])
            session.add(s1)
        session.commit()
class Audit_table(Base):
    __tablename__ = 'Audit_table'
    audit_id = Column(Integer, primary_key=True, autoincrement=True)  # Indicates the no of changes done in my table
    BusinessYear= Column(Integer,ForeignKey('Service_area'))
    start_time = Column(String(1000))
    end_time = Column(String(1000))
    no_of_records=Column(Integer)
    transaction_status=Column(String(1000))
Base.metadata.create_all(engine)
confirm=str(input("Do you want to transform load:{yes/no)\n"))

if (confirm == "yes"):
    mod_year=int(input("enter a year to insert(2016,2015,2014)"))#NOte please enter the earlier loaded table
    start = datetime.now()
    # Checking Dataset is already available
    dat_check = session.query(Service_area).filter_by(BusinessYear=year).first()
    if dat_check is not None:
        session.delete(dat_check)
        print("Old Records Deleted")
    flag = 0
    checkpoint = 0
    total_records = 0
    try:
        if (mod_year == int(2014)):
            with open(r'C:\Users\DELL\PycharmProjects\pythonProject\input\2014\Service_Area_PUF2014.csv', 'r',
                      encoding='utf-8') as f:
                Dictreader = csv.DictReader(f)
                print("ok")
                for row in Dictreader:
                    x = dict(row)
                    try:
                        record = Service_area(BusinessYear=x["BusinessYear"], StateCode=x["StateCode"],
                                              IssuerId=x["IssuerId"], SourceName=x["SourceName"],
                                              VersionNum=x["VersionNum"], IssuerId2=x["IssuerId2"],
                                              ServiceAreaId=x["ServiceAreaId"],
                                              ServiceAreaName=x["ServiceAreaName"],
                                              CoverEntireState=x["CoverEntireState"], County=x["County"],
                                              PartialCounty=x["PartialCounty"],
                                              ZipCodes=x["ZipCodes"],
                                              PartialCountyJustification=x["PartialCountyJustification"],
                                              RowNumber=x["RowNumber"],
                                              DentalOnly=x["DentalOnly"], MarketCoverage=x["MarketCoverage"],
                                              ImportDate=x["ImportDate"])
                        session.add(record)
                        flag = flag + 1

                    except  KeyboardInterrupt as k:
                        Service_area.__table__.drop(engine)
                        print("Process aborted")
                        break
                    except MemoryError as e:
                        session.rollback()
                        Service_area.__table__.drop(engine)
                        session.add(Audit_table(Businessyear=year, start_time=start, end_time=datetime.now(),
                                                no_of_records=total_records, transaction_status="Failure"))
                        session.commit()
                        print("Transaction roll back")
                        break
                else:
                    total_records += flag
                    session.commit()
                    session.add(Audit_table(BusinessYear=mod_year, start_time=start, end_time=datetime.now(),
                                            no_of_records=total_records, transaction_status="Success"))
                    session.commit()
                    print("All records inserted: Transaction Completed")

        elif (mod_year==int(2015)):
            with open(r'C:\Users\DELL\PycharmProjects\pythonProject\input\2015\Service_Area_PUF2015.csv', 'r',
                      encoding='utf-8') as f:
                Dictreader = csv.DictReader(f)
                print("ok")
                for row in Dictreader:
                    x = dict(row)
                    try:
                        record = Service_area(BusinessYear=x["BusinessYear"], StateCode=x["StateCode"],
                                              IssuerId=x["IssuerId"], SourceName=x["SourceName"],
                                              VersionNum=x["VersionNum"], IssuerId2=x["IssuerId2"],
                                              ServiceAreaId=x["ServiceAreaId"],
                                              ServiceAreaName=x["ServiceAreaName"],
                                              CoverEntireState=x["CoverEntireState"], County=x["County"],
                                              PartialCounty=x["PartialCounty"],
                                              ZipCodes=x["ZipCodes"],
                                              PartialCountyJustification=x["PartialCountyJustification"],
                                              RowNumber=x["RowNumber"],
                                              DentalOnly=x["DentalOnly"], MarketCoverage=x["MarketCoverage"],
                                              ImportDate=x["ImportDate"])
                        session.add(record)
                        flag = flag + 1

                    except  KeyboardInterrupt as k:
                        Service_area.__table__.drop(engine)
                        print("Process aborted")
                        break
                    except MemoryError as e:
                        session.rollback()
                        Service_area.__table__.drop(engine)
                        session.add(Audit_table(Businessyear=year, start_time=start, end_time=datetime.now(),
                                                no_of_records=total_records, transaction_status="Failure"))
                        session.commit()
                        print("Transaction roll back")
                        break
                else:
                    total_records += flag
                    session.commit()
                    session.add(Audit_table(BusinessYear=mod_year, start_time=start, end_time=datetime.now(),
                                            no_of_records=total_records, transaction_status="Success"))
                    session.commit()
                    print("All records inserted: Transaction Completed")
        else:
            with open(r'C:\Users\DELL\PycharmProjects\pythonProject\input\2016\Service_Area_PUF2016.csv','r',encoding='utf-8') as f:

                Dictreader = csv.DictReader(f)
                print("ok")
                for row in Dictreader:
                    x = dict(row)
                    try:
                        record = Service_area(BusinessYear=x["BusinessYear"], StateCode=x["StateCode"],
                                              IssuerId=x["IssuerId"], SourceName=x["SourceName"],
                                              VersionNum=x["VersionNum"], IssuerId2=x["IssuerId2"],
                                              ServiceAreaId=x["ServiceAreaId"],
                                              ServiceAreaName=x["ServiceAreaName"],
                                              CoverEntireState=x["CoverEntireState"], County=x["County"],
                                              PartialCounty=x["PartialCounty"],
                                              ZipCodes=x["ZipCodes"],
                                              PartialCountyJustification=x["PartialCountyJustification"],
                                              RowNumber=x["RowNumber"],
                                              DentalOnly=x["DentalOnly"], MarketCoverage=x["MarketCoverage"],
                                              ImportDate=x["ImportDate"])
                        session.add(record)
                        flag = flag + 1

                    except  KeyboardInterrupt as k:
                        Service_area.__table__.drop(engine)
                        print("Process aborted")
                        break
                    except MemoryError as e:
                        session.rollback()
                        Service_area.__table__.drop(engine)
                        session.add(Audit_table(Businessyear=year, start_time=start, end_time=datetime.now(),
                                                no_of_records=total_records, transaction_status="Failure"))
                        session.commit()
                        print("Transaction roll back")
                        break
                else:
                    total_records += flag
                    session.commit()
                    session.add(Audit_table(BusinessYear=mod_year, start_time=start, end_time=datetime.now(),
                                            no_of_records=total_records, transaction_status="Success"))
                    session.commit()
                    print("All records inserted: Transaction Completed")




    except UnicodeDecodeError as e:
        Service_area.__table__.drop(engine)
        print("Check encoding")


    except FileNotFoundError as e:
        print("Dataset for Year doesnt exist")








