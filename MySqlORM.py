# -*- coding: utf-8 -*-
"""
    wrapper of sqlalchemy.

    show variables like "%char%";
    SET character_set_client='gbk';
    SET character_set_connection='gbk';
    SET character_set_database='utf8';
    SET character_set_results='gbk';
    SET character_set_server='utf8';
    SET character_set_system='utf8';
"""

from sqlalchemy import *
from PwLogging import PwLogging
from sqlalchemy.pool import NullPool
from sqlalchemy.orm import *

class FlaskORM(object):
    def __init__(self, host, user, pwd, tlist, tclass, db=None, port=3306, logger=None, redo=True):
        self.error = 0
        self.host = host
        self.user = user
        self.passwd = pwd
        self.tlist = tlist
        self.tclass= tclass
        self.db = db
        self.port = port
        self.logger = logger
        self.redo = redo
        self.__connect()

    def __del__(self):
        self.Close()

    def tableLoad(self, list):
        dic = {}
        for tmp in list:
            dic[tmp] = Table(tmp, self.metadata, autoload=True)
        return dic
    def makeClass(self, class_name):
        class C(object):
            pass
        C.__name__ = class_name
        return C

    def makeClassList(self, classNameList):
        dic = {}
        for className in classNameList:
            dic[className] = self.makeClass(className)
        return dic

    def tableMapper(self, classNameDic, tableDic, tablelist):
        for tableName in tablelist:
            mapper(classNameDic[tableName.upper()], tableDic[tableName])

    def __connect(self):
        """
        self.tdic = self.tableLoad(self.tlist)
        self.tclass = self.makeClassList(self.tclass)
        self.tableMapper(self.tclass, self.tdic, self.tlist)
        """
        try:
            CENGINE_URL = "mysql://%s:%s@%s:%s/%s?charset=utf8" \
                          % (self.user, self.passwd, self.host, self.port, self.db)
            self.engine = create_engine(CENGINE_URL, poolclass=NullPool, echo=True)
            self.metadata = MetaData(self.engine)
            self.tdic = self.tableLoad(self.tlist)
            self.tclass = self.makeClassList(self.tclass)
            self.tableMapper(self.tclass, self.tdic, self.tlist)
        except Exception, e:
            self.error = 1
            if self.logger:
                self.logger.exception("%s" % e)
            else:
                raise

    def insert_bef(self, className):
        """
        :param className:
        :return: the mapper class
        """
        mapperClass = self.tclass[className]()
        return mapperClass

    def insert_val(self, mapperClass):
        """
        :param mapperClass: get the new value
        :return:
        """
        Session = sessionmaker(bind=self.engine)
        session = Session()
        #t = self.tclass[tclass]()
        session.add(mapperClass)
        session.flush()
        session.commit()

    def guuidMysql(self):
        try:
            Session = sessionmaker(bind=self.engine)
            session = Session()
            number = session.query(func.guuid('')).scalar()
            return number
        except Exception, e:
            self.logger.exception("%s" % e)
        finally:
            session.close()

    def selectMax(self, tableColumn, match=1):
        try:
            Session = sessionmaker(bind=self.engine)
            session = Session()
            maxval = session.query(func.max(tableColumn)).filter(match).scalar()
            return maxval
        except Exception, e:
            self.logger.exception("%s" % e)
        finally:
            session.close()

    def Close(self):
        self.engine.dispose()

if __name__ == '__main__':
    db_address = 'localhost'
    db_username = 'root'
    db_password = 'a2RrajEyMw=='
    db = 'kdserver'

    list = 'test, app_all_brands, app_appraise_object, app_appraise_point, app_first_show, app_member, app_order, app_order_details, app_const_brands, app_const_category'
    #tlist = ['test', 'app_all_brands']
    tlist = list.split(', ')
    tclass = list.upper().split(', ')

    LOGDIR = '/var/kdserver/log'
    logger = PwLogging(dir=LOGDIR)
    logger.addRotatingFileHandler('ORMtest.log', level='debug', fmt='simple')

    mysql_orm = FlaskORM(host=db_address, user=db_username, pwd=db_password, tlist=tlist, tclass=tclass, db=db, logger=logger)
    session = create_session()
    """
    tmpQuery = session.query(mysql_orm.tclass['TEST']).filter_by(name='p1').first()
    print tmpQuery
    num = mysql_orm.engine.execute("insert into test (img, password) values('123123', '12312313123123')")
    print num



    t = mysql_orm.insert_bef('TEST')
    t.name = '123123'
    mysql_orm.insert_val(t)

    number = mysql_orm.guuidMysql()
    print number
"""