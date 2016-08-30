# MySQL_wrapper

#PwLogging
日志封装

from PwLogging import PwLogging

ex:
  LOGDIR = '/var/test'
  logger = PwLogging(dir=LOGDIR)
  logger.addRotatingFileHandler('test.log',level='debug',fmt='simple')

  logger.debug("SQL 00: %s" % sql)

#PwMysql
需要代入SQL语句 执行

ex:
  from PwMysql import PwMysql 
  mysql = PwMysql(host=db_address,user=db_username,passwd=db_password,db='psms',logger=logger)


#MySql_RAW_SQL
自动映射数据库，参数代替SQL语句执行

#MySqlORM
自动映射数据库，自动ORM


缺陷：
  调用func执行需要在内部新建seesion
  
  




