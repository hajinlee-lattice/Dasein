__author__ = 'nxu'
import logging
import time
import os

def GetLoggerFilePath(logFolder,Logtype):
        time_stamp=time.strftime("%Y%m%d", time.localtime())
        full_fileName=logFolder+'\\'+str(Logtype)+'_'+str(time_stamp)+'.txt'
        print full_fileName
        if not (os.path.isfile(full_fileName)):
            print 'create on new file'
            f=open(full_fileName,'w')
            f.close()
        return full_fileName

class Logger(object):
    logger=''
    PrintConsole=False
    @staticmethod
    def setInfoLog(msg):
        #print 'log info'
        if Logger.PrintConsole:
            print msg
        Logger.logger.setLevel(logging.INFO)
        Logger.logger.info(msg)

    @staticmethod
    def setErrorLog(msg):
        if Logger.PrintConsole:
            print msg
        #self.logger.setLevel(logging.ERROR)
        Logger.logger.error(msg)

    @staticmethod
    def setExceptionLog(msg):
        if Logger.PrintConsole:
            print msg
        Logger.logger.exception(msg)

    @staticmethod
    def SetLogger(logFolder,logtype):
        Logger.logfile=GetLoggerFilePath(logFolder,logtype)
        Logger.logger=logging.getLogger()
        hdlr=logging.FileHandler(Logger.logfile)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        hdlr.setFormatter(formatter)
        Logger.logger.addHandler(hdlr)


#if __name__ == '__main__':
    #print 'start call setproperoties'
    #lg=Logger('TestLog')
    #lg.setErrorLog('error message -1')
    #lg.setInfoLog('Info message ---3')
    #lg.setExceptionLog('exception message--2')
    #Logger.SetLogger('DanteTest')
    #ogger.setInfoLog('info message --1')
    #Logger.setErrorLog('error message --2')
    #try:
        #f=open('aaaaaaa.sss','rb')
    #except Exception:
        #print '123'
       # Logger.setExceptionLog('exception occure--3')
