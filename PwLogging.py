# coding=utf-8

""" wrapper of logging. """

import os
import logging, logging.handlers

class PwLogging:
    def __init__(self, name=None,dir="/var/log"):
        # clear root logger handlers
        logging.getLogger().handlers = [];
        
        self.logger      = logging.getLogger(name)
        self.__logdir    = dir
        
        self.__formatters = {'detail' :'%(asctime)s - %(name)s[%(levelname)s] - [%(pathname)s:%(lineno)d] - [%(module)s %(process)d %(thread)d]: %(message)s',
                             'verbose':'%(asctime)s - %(name)s[%(levelname)s] - [%(pathname)s:%(lineno)d]: %(message)s',
                             'simple' :'%(asctime)s - %(name)s[%(levelname)s]: %(message)s'}
        
        self.__levels = {'notset'  : logging.NOTSET,
                         'debug'   : logging.DEBUG,
                         'info'    : logging.INFO,
                         'warning' : logging.WARNING,
                         'error'   : logging.ERROR,
                         'critical': logging.CRITICAL}
                        
        self.logger.setLevel(logging.DEBUG)
        
    def __del__(self):
        del self.__formatters
        del self.__logdir
        del self.__levels
        
    def addStreamHandler(self, level=None, fmt=None):
        hdr = logging.StreamHandler()
        
        formatter = logging.Formatter(self.__formatters.get(fmt,self.__formatters['verbose']))
        hdr.setFormatter(formatter)
        hdr.setLevel(self.__levels.get(level, self.__levels['info']))
        
        self.logger.addHandler(hdr)
    
    def addFileHandler(self, fname, dir=None, level=None, fmt=None):
        logdir = self.__logdir
        if dir and isinstance(dir, basestring):
            dir = os.path.dirname(dir)
            if len(dir) > 0 and not os.path.exists(dir):
                os.makedirs(dir)
                logdir = dir
                
        logfile = os.path.join(logdir,fname)        
        hdr = logging.FileHandler(logfile)
        
        formatter = logging.Formatter(self.__formatters.get(fmt,self.__formatters['verbose']))
        hdr.setFormatter(formatter)
        hdr.setLevel(self.__levels.get(level, self.__levels['info']))
                
        self.logger.addHandler(hdr)

    def addTimedRotatingFileHandler(self, fname, level=None, dir=None, fmt=None):
        logdir = self.__logdir
        if dir and isinstance(dir, basestring):
            dir = os.path.dirname(dir)
            if len(dir) > 0 and not os.path.exists(dir):
                os.makedirs(dir)
                logdir = dir
        
        logfile = os.path.join(logdir,fname)
                
        hdr = logging.handlers.TimedRotatingFileHandler(logfile, 'midnight', 1, 0)
        
        formatter = logging.Formatter(self.__formatters.get(fmt,self.__formatters['verbose']))
        hdr.setFormatter(formatter)
        hdr.setLevel(self.__levels.get(level, self.__levels['info']))
        hdr.suffix = "%Y%m%d"
        
        self.logger.addHandler(hdr)
        
    def addDatagramHandler(self, host, port, level=None, fmt=None):
        hdr = logging.handlers.DatagramHandler(host, port)
        
        formatter = logging.Formatter(self.__formatters.get(fmt,self.__formatters['verbose']))            
        hdr.setFormatter(formatter)
        hdr.setLevel(self.__levels.get(level, self.__levels['info']))
        
        self.logger.addHandler(hdr)
        
    def addSocketHandler(self, host, port, level=None, fmt=None):
        hdr = logging.handlers.SocketHandler(host, port)
        
        formatter = logging.Formatter(self.__formatters.get(fmt,self.__formatters['verbose']))
        hdr.setFormatter(formatter)
        hdr.setLevel(self.__levels.get(level, self.__levels['info']))
        
        self.logger.addHandler(hdr)
    
    def addSysLogHandler(self,level=None, fmt=None,address=('localhost',logging.handlers.SYSLOG_UDP_PORT), facility=1):
        hdr = logging.handlers.SysLogHandler(address, facility)
        
        formatter = logging.Formatter(self.__formatters.get(fmt,self.__formatters['verbose']))    
        hdr.setFormatter(formatter)
        hdr.setLevel(self.__levels.get(level, self.__levels['info']))
        
        self.logger.addHandler(hdr)
    
    def addRotatingFileHandler(self,fname,m='a',ms=20*1024*1024,bc=3, level=None, fmt=None):
        fname=self.generate_path(self.__logdir,fname)
        hdr = logging.handlers.RotatingFileHandler(fname,mode=m,maxBytes=ms,backupCount=bc)
        formatter = logging.Formatter(self.__formatters.get(fmt,self.__formatters['verbose']))    
        hdr.setFormatter(formatter)
        hdr.setLevel(self.__levels.get(level, self.__levels['info']))
        self.logger.addHandler(hdr)
    
    def generate_path(self,dir,file_name):
    	  if os.path.exists(dir) and os.path.isdir(dir):
    	      return os.path.join(dir,file_name)
    	  else:
    	      os.makedirs(dir)
    	      return os.path.join(dir,file_name)	  
    def debug(self, msg, *args, **kwargs):
        self.logger.debug(msg, *args, **kwargs)
    def info(self, msg, *args, **kwargs):
        self.logger.info(msg, *args, **kwargs)
    def critical(self, msg, *args, **kwargs):
        self.logger.debug(msg, *args, **kwargs)
    def warning(self, msg, *args, **kwargs):
        self.logger.warning(msg, *args, **kwargs)
    def error(self, msg, *args, **kwargs):
        self.logger.error(msg, *args, **kwargs)
    def log(self,lvl, msg, *args, **kwargs):
        self.logger.log(lvl, msg, *args, **kwargs)
    def exception(self,msg, *args):
        self.logger.exception(msg, *args)
        
        