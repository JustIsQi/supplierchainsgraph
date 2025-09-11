# -*- coding: UTF-8 -*-

from sqlalchemy import Table, create_engine, MetaData
from sqlalchemy.orm import sessionmaker, scoped_session, declarative_base
import pymysql
import pandas as pd


engine = create_engine('mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=utf8&autocommit=true'.format(
    'wind_admin',
    'ELPWN2YJRXBCQKYd',
    '10.100.0.28',
    3306,
    "winddb"
),
    pool_pre_ping=True,
    pool_recycle=1200,
    connect_args={'connect_timeout': 30, "read_timeout": 30}
)


Base = declarative_base()
DBSession = scoped_session(sessionmaker(bind=engine))
metadata = MetaData()
session = DBSession()

class AShareDescription(Base):
    __table__ = Table('ASHAREDESCRIPTION', metadata,  autoload_with=engine)

class AShareAnnColumn(Base):
    __table__ = Table('ASHAREANNCOLUMN', metadata,  autoload_with=engine)


company_name_dict = {}
ann_type_dict = {}
def get_company_name(s_info_windcode):
    company_info = company_name_dict.get(s_info_windcode)
    if company_info:
        return company_info
    else:
        try:
            std_data = session.query(AShareDescription.S_INFO_NAME,AShareDescription.S_INFO_COMPNAME).filter(AShareDescription.S_INFO_WINDCODE == s_info_windcode).first()
            if std_data:
                company_info = (std_data.S_INFO_NAME,std_data.S_INFO_COMPNAME)
                company_name_dict[s_info_windcode] = company_info
                return company_info
            else:
                return None
        except Exception as e:
            print(e)
            session.rollback()

def get_type_name(n_info_fcode):
    if n_info_fcode:
        type_name = ann_type_dict.get(n_info_fcode)
        if type_name:
            return type_name
        else:
            try:
                std_data = session.query(AShareAnnColumn.N_INFO_NAME).filter(AShareAnnColumn.N_INFO_FCODE == n_info_fcode).first()
                if std_data:
                    ann_type_dict[n_info_fcode] = std_data.N_INFO_NAME
                    return std_data.N_INFO_NAME
                else:
                    return None
            except Exception as e:
                print(e)
                session.rollback()
    session.bind.dispose()


def chinascope_search(sql):
    # 建立数据库连接
    conn = pymysql.connect(
        host='10.100.0.28',
        port=3306,
        user='chinascope_admin', 
        password='PYB9pebc4qBdaZ',
        database='chinascope'
    )

    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        # 获取所有数据
        data = cursor.fetchall()
        # 获取列名
        columns = [desc[0] for desc in cursor.description]
        # 转换为DataFrame
        data_list = pd.DataFrame(list(data), columns=columns).values.tolist()
        return data_list
    except Exception as e:
        print(f'发生错误: {e}')
    finally:
        # 关闭游标和连接
        cursor.close()
        conn.close()

def wind_search(sql):
    # 建立数据库连接
    conn = pymysql.connect(
        host='10.100.0.28',
        port=3306,
        user='wind_admin', 
        password='ELPWN2YJRXBCQKYd',
        database='winddb'
    )

    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        # 获取所有数据
        data = cursor.fetchall()
        # 获取列名
        columns = [desc[0] for desc in cursor.description]
        # 转换为DataFrame
        data_list = pd.DataFrame(list(data), columns=columns).values.tolist()
        return data_list
    except Exception as e:
        print(f'发生错误: {e}')
    finally:
        # 关闭游标和连接
        cursor.close()
        conn.close()

# if __name__ == '__main__':
#     print(get_company_name("300168.SZ"))
#     print(get_type_name("5506010000"))
#     # 关闭连接,使用服务时无需关闭
#     session.bind.dispose() 
