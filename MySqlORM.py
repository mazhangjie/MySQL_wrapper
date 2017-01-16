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

import time
from sqlalchemy import *
from PwLogging import PwLogging
from sqlalchemy.pool import NullPool
from sqlalchemy.orm import *

class FlaskORM(object):
    def __init__(self, host, user, pwd, db, port=3306, logger=None, redo=True):
        self.error = 0
        self.host = host
        self.user = user
        self.passwd = pwd
        self.db = db
        self.port = port
        self.logger = logger
        self.redo = redo
        self.__connect()

    def __del__(self):
        self.Close()

    def tableList(self, databaseName):
        result = mysql_orm.engine.execute("show tables from %s;" % databaseName)
        if result:
            tablesList = []
            for row in result:
                tablesList.append(row[0])
            return tablesList
        else:
            #"the database no table"
            return None

    def tableUpper(self, lowerList):
        upperList = []
        for tableName in lowerList:
            upperList.append(tableName.upper())
        return upperList

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
            self.tlist = self.tableList(self.db)
            self.tclass = self.tableUpper(self.tlist)
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

#    def guuidMysql(self):
#        try:
#            Session = sessionmaker(bind=self.engine)
#            session = Session()
#            number = session.query(func.guuid('')).scalar()
#            return number
#        except Exception, e:
#            self.logger.exception("%s" % e)
#        finally:
#            session.close()
#
#    def selectMax(self, tableColumn, match=1):
#        try:
#            Session = sessionmaker(bind=self.engine)
#            session = Session()
#            maxval = session.query(func.max(tableColumn)).filter(match).scalar()
#            return maxval
#        except Exception, e:
#            self.logger.exception("%s" % e)
#        finally:
#            session.close()
#
    def getSession(self):
        try:
            Session = sessionmaker(bind=self.engine)
            session = Session()
            return session
        except Exception, e:
            self.logger.exception("%s" % e)

    def Close(self):
        self.engine.dispose()

if __name__ == '__main__':
    db_address = 'localhost'
    db_username = 'root'
    db_password = 'a2RrajEyMw=='
    db = 'kdserver'

    #list = 'app_member, app_order, app_order_details, app_const_listStatus'
    #tlist = ['test', 'app_all_brands']
    #tlist = list.split(', ')
    #tclass = list.upper().split(', ')

    LOGDIR = '/var/kdserver/log'
    logger = PwLogging(dir=LOGDIR)
    logger.addRotatingFileHandler('ORMtest.log', level='debug', fmt='simple')

    #mysql_orm = FlaskORM(host=db_address, user=db_username, pwd=db_password, tlist=tlist, tclass=tclass, db=db, logger=logger)
    mysql_orm = FlaskORM(host=db_address, user=db_username, pwd=db_password, db=db, logger=logger)
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

    session.query(mysql_orm.tclass['APP_ORDER_DETAILS']).filter_by(orderId=74).update({'picStatusId':None, 'picStatus': '2222'})
    #session.commit()
    tmpQuery = session.query(mysql_orm.tclass['APP_ORDER_DETAILS']).filter_by(orderId=74).first()
    print tmpQuery.picStatusId
"""
    result = mysql_orm.engine.execute("show tables from test;")
    if result:
        tablesList = []
        for row in result:
            tablesList.append(row[0])
    else:
        print "the database no table"
