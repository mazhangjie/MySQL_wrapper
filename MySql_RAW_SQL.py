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
from sqlalchemy.exc import StatementError
from PwLogging import PwLogging
#from sqlalchemy.databases.mysql import *
from sqlalchemy.pool import NullPool

class FlaskORM(object):
    def __init__(self, host, user, pwd, tlist, db=None, port=3306, logger=None, redo=True):
        self.error = 0
        self.host = host
        self.user = user
        self.passwd = pwd
        self.tlist = tlist
        self.db = db
        self.port = port
        self.logger = logger
        self.redo = redo
        self.__connect()

    def __del__(self):
        self.Close()

    def selectDb(self, db):
        pass

    def tableMapper(self, list):
        dic = {}
        for tmp in list:
            dic[tmp] = Table(tmp, self.metadata, autoload=True)
        return dic

    def __connect(self):
        try:
            CENGINE_URL = "mysql://%s:%s@%s:%s/%s?charset=utf8" \
                          % (self.user, self.passwd, self.host, self.port, self.db)
            self.engine = create_engine(CENGINE_URL, poolclass=NullPool, echo=False)
            self.metadata = MetaData(self.engine)
            self.tdic = self.tableMapper(self.tlist)
            self.conn = self.engine.connect()
        except Exception, e:
            self.error = 1
            if self.logger:
                self.logger.exception("%s" % e)
            else:
                raise
    def twoListToDic(self, listTuple):
        num, match = 0, {}
        if len(listTuple) == 2 and len(listTuple[0]) == len(listTuple[1]):
            while num < len(listTuple[0]):
                match[listTuple[0][num]] = listTuple[1][num]
                num += 1
        return match

    def twoListToAndStr(self, listTuple):
        num, match = 0, []
        if len(listTuple) == 2 and len(listTuple[0]) == len(listTuple[1]):
            while num < len(listTuple[0]):
                match.append(listTuple[0][num] + ' = "' + listTuple[1][num] + '"')
                num += 1
        return match

    def insert_val(self, table, *andlist):
        try:
            match = self.twoListToDic(andlist)
            r = self.conn.execute(table.insert(), match)
            return r.rowcount
        except StatementError, e:
            self.error = 1
            if self.redo:
                self.__connect()
                time.sleep(0.5)
            if self.logger:
                self.logger.exception("%s" % e)
            else:
                raise
    def queryRowsAll(self, table):
        try:
            r = self.conn.execute(select([table]))
            return r.fetchall()
        except StatementError, e:
            self.error = 1
            if self.redo:
                self.__connect()
                time.sleep(0.5)
            if self.logger:
                self.logger.exception("%s" % e)
            else:
                raise

    def queryRows(self, table, *andlist):
        try:
            match = self.twoListToAndStr(andlist)
            r = self.conn.execute(select([table]).where(' and '.join(match)))
            return r.fetchall()
        except StatementError, e:
            self.error = 1
            if self.redo:
                self.__connect()
                time.sleep(0.5)
            if self.logger:
                self.logger.exception("%s" % e)
            else:
                raise

    def queryRowsLimit(self, table, column, start_num, read_num):
        try:
            r = self.conn.execute(select([table]).order_by(table.c[column]).limit(start_num).offset(read_num))
            return r.fetchall()
        except StatementError, e:
            self.error = 1
            if self.redo:
                self.__connect()
                time.sleep(0.5)
            if self.logger:
                self.logger.exception("%s" % e)
            else:
                raise

    def queryRow(self, table, *andlist):
        try:
            match = self.twoListToAndStr(andlist)
            r = self.conn.execute(select([table]).where( ' and '.join(match)))
            tmp = r.fetchone()
            if tmp :
                return tmp[0]
            else:
                return tmp
        except StatementError, e:
            self.error = 1
            if self.redo:
                self.__connect()
                time.sleep(0.5)
            if self.logger:
                self.logger.exception("%s" % e)
            else:
                raise

    def update_val(self, table, column, match_val, *andlist):
        try:
            match = self.twoListToDic(andlist)
            r = self.conn.execute(table.update().where(table.c[column] == match_val).values(match))
            return r.rowcount
        except StatementError, e:
            self.error = 1
            if self.redo:
                self.__connect()
                time.sleep(0.5)
            if self.logger:
                self.logger.exception("%s" % e)
            else:
                raise

    def Close(self):
        self.conn.close()
        self.engine.dispose()

if __name__ == '__main__':
    db_address = 'localhost'
    db_username = 'root'
    db_password = 'a2RrajEyMw=='
    db = 'kdserver'
    tlist = ['test', 'app_all_brands']

    LOGDIR = '/var/kdserver/log'
    logger = PwLogging(dir=LOGDIR)
    logger.addRotatingFileHandler('ORMtest.log', level='debug', fmt='simple')

    mysql_orm = FlaskORM(host=db_address, user=db_username, pwd=db_password, tlist=tlist, db=db, logger=logger)
    #print mysql_orm.tdic['test']
    #print mysql_orm.tdic['test'].c
    #print mysql_orm.tdic['app_all_brand']
    #print mysql_orm.tdic['app_all_brand'].c
    #insert
    #r = mysql_orm.insert_val(mysql_orm.tdic['test'], ['img', 'name', 'password'], ['test11111','哈哈哈哈', '12312311123'])

    #update
    #r = mysql_orm.update_val(mysql_orm.tdic['test'], 'id' ,'7',['img', 'name', 'password'], ['tes','p1', '13'])

    #select all
    ##[(1L, u'eeerere', u'lili', u'6666'), (2L, u'qweqwe', u'jack', u'77777')]
    #r = mysql_orm.queryRowsAll(mysql_orm.tdic['test'])

    #[(2L, u'qweqwe', u'jack', u'77777'), (4L, u'qweqwe', u'jack', u'123123123')]
    #r = mysql_orm.queryRows(mysql_orm.tdic['test'], ['img', 'name'], ['qweqwe', 'jack'])


    #[(2L,), (4L,)]
    #r = mysql_orm.queryRows(mysql_orm.tdic['test'].c['id'], ['img', 'name'], ['qweqwe', 'jack'])

    #[(u'qweqwe',), (u'222',)]
    #r = mysql_orm.queryRows(mysql_orm.tdic['test'].c['name'], ['img'], ['test11111'])
    #r = mysql_orm.queryRows(mysql_orm.tdic['test'], ['img'], ['test11111'])

    #select one
    ##value
    #imgstr = 'qweqwe'
    #r = mysql_orm.queryRow(mysql_orm.tdic['test'].c['id'], ['img', 'name', 'password'], [imgstr, 'mile', '11122'])

    ##value
    #idstr = '1'
    #r = mysql_orm.queryRow(mysql_orm.tdic['test'].c['img'], ['id'], [idstr])

    #
    #  .encode('gbk')

    #print r

    """
    try:
        mysql_orm.engine.connect()
    except sqlalchemy.exc.OperationalError, e:
        backupWriteLog('MySQL Connection Fild !')
    """
    """
    try:
         mysql_orm.tdic
    except AttributeError, e:
        backupWriteLog('MySQL tables load faild !')
    """

    mysql_orm.__del__()

"""
#insert
u = dict(unclick_img = 'eeerere')
r = conn.execute(test.insert(), u)
print r.rowcount

#select
r = conn.execute(select([test]))
print r.rowcount
ru = r.fetchall()
print ru


#update
r = conn.execute(test.update().where(test.c.id == '9').values(unclick_img = 'tttttt'))
print r.rowcount

#delete
#r= conn.execute(test.delete(test.c.id == 1))
#print r.rowcount

"""