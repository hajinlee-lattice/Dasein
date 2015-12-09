__author__ = 'BWang'
import logging,time

class LogFactory():
    @classmethod
    def getLog(cls,name="",alsoToConsole=False):
        logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='.\\PlaymakerEnd2End\\log\\'+name+'%s.log'%time.strftime("%Y%m%d%H%M%S"),
                filemode='w')
        log=logging.getLogger(name)
        if alsoToConsole:
            console = logging.StreamHandler()
            console.setLevel(logging.INFO)
            formatter = logging.Formatter('%(name)-9s: %(levelname)-10s %(message)s')
            console.setFormatter(formatter)
            log.addHandler(console)
        return log
