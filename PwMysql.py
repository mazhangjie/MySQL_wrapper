# coding=utf-8

""" wrapper of MySQLdb. """

import MySQLdb
import time
from warnings import filterwarnings
filterwarnings('error', category = MySQLdb.Warning)

class PwMysql:
    def __init__(self,host,user,passwd,db=None,port=3306,logger=None,redo=True):
        self.error = 0
        self.host = host
        self.user = user
        self.passwd = passwd
        self.db = db
        self.port = port
        self.logger = logger
        self.redo = redo
        self.__connect()
    
    def __del__(self):
        self.close()
    
    def selectDb(self,db):
        try:
            self.db = db
            self.conn.select_db(db)
        except MySQLdb.Error, e:
            self.error = 1
            if self.logger:
                self.logger.exception("%s" % (e))
            else:
                raise
    
    def __connect(self):
        try:
            self.conn = MySQLdb.connect(host=self.host,port=self.port,user=self.user,passwd=self.passwd)
            if self.db:
                self.conn.select_db(self.db)
            self.conn.autocommit(True)
            self.cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        except MySQLdb.Error, e:
            self.error = 1
            if self.logger:
                self.logger.exception("%s" % (e))
            else:
                raise
        
    def queryCount(self,sql):
        try:
            rowcnt = 0
            if self.query(sql) > 0:
                rowcnt = self.cur.rowcount
            return rowcnt
        except MySQLdb.Warning,w:
            if self.logger:
                self.logger.warning("%s" % (w))
        except MySQLdb.Error, e:
            self.error = 1
            if self.redo:
                self.__connect()
                time.sleep(0.5)
            if self.logger:
                self.logger.exception("%s" % (e))
            else:
                raise
    
    def queryRow(self,sql):
        try:
            row = None
            if self.cur.execute(sql) > 0:
                row = self.cur.fetchone()
            return row
        except MySQLdb.Warning,w:
            if self.logger:
                self.logger.warning("%s" % (w))
        except MySQLdb.Error, e:
            self.error = 1
            if self.redo:
                self.__connect()
                time.sleep(0.5)
            if self.logger:
                self.logger.exception("%s" % (e))
            else:
                raise
    
    def queryRows(self,sql):
        try:
            rows = None
            if self.cur.execute(sql) > 0:
                rows = self.cur.fetchall()
            return rows
        except MySQLdb.Warning,w:
            if self.logger:
                self.logger.warning("%s" % (w))
        except MySQLdb.Error, e:
            self.error = 1
            if self.redo:
                self.__connect()
                time.sleep(0.5)
            if self.logger:
                self.logger.exception("%s" % (e))
            else:
                raise
    
    def insert(self,sql,params=None,eflag=False):
        try:
            self.cur.execute(sql,params)
            lastid = self.cur.lastrowid
            return lastid
        except MySQLdb.Warning,w:
            if self.logger:
                self.logger.warning("%s" % (w))
        except MySQLdb.ProgrammingError,e:
            if eflag == True:
                pass
            else:
                if self.logger:
                    self.logger.exception("%s" % (e))
                else:
                    raise
        except MySQLdb.Error, e:
            self.error = 1
            if self.redo:
                self.__connect()
                time.sleep(0.5)
            if self.logger:
                self.logger.exception("%s" % (e))
            else:
                raise
    
    def update(self,sql,params=None,eflag=False):
        try:
            self.cur.execute(sql,params)
        except MySQLdb.Warning,w:
            if self.logger:
                self.logger.warning("%s" % (w))
        except MySQLdb.ProgrammingError,e:
            if eflag == True:
                pass
            else:
                if self.logger:
                    self.logger.exception("%s" % (e))
                else:
                    raise
        except MySQLdb.Error, e:
            self.error = 1
            if self.redo:
                self.__connect()
                time.sleep(0.5)
            if self.logger:
                self.logger.exception("%s" % (e))
            else:
                raise
    
    def close(self):
        if hasattr(self,'cur'):
            self.cur.close()
        if hasattr(self,'conn'):
            self.conn.close()
            
    def check_conn(self):
        try:
            self.conn.ping()
            return 1
        except Exception, e:
            self.logger.exception("%s" % (e))
            return 0
            