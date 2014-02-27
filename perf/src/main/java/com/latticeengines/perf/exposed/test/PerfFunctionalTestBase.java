package com.latticeengines.perf.exposed.test;

import java.io.File;
import java.io.IOException;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.perf.exposed.metric.sink.SwitchableFileSink;
import com.latticeengines.perf.exposed.util.SecureFileTransferAgent;


public class PerfFunctionalTestBase {
    private static final Log log = LogFactory.getLog(PerfFunctionalTestBase.class);
    
    private SecureFileTransferAgent sftp = null;
    private String watchDir = null;
    private String metricFile = null;
    private boolean startInvoked = false;
    private boolean stopInvoked = false;
    
    public PerfFunctionalTestBase(String serverAddress, String userId, String password) {
        try {
            Configuration conf = new PropertiesConfiguration("hadoop-metrics2.properties").interpolatedConfiguration();
            watchDir = (String) conf.getProperty("ledpjob.sink.file.watchdir");
            metricFile = (String) conf.getProperty("ledpjob.sink.file.filename");
            
        } catch (ConfigurationException e) {
            throw new IllegalStateException(e);
        }
        sftp = new SecureFileTransferAgent(serverAddress, userId, password);
    }

    public void beforeClass() {
        startInvoked = invoke("beforeClass", startInvoked, true);
    }
    
    public void afterClass() {
        stopInvoked = invoke("afterClass", stopInvoked, false);
        sftp.fileTransfer(new File(metricFile).getName(), metricFile, SecureFileTransferAgent.FileTransferOption.DOWNLOAD);    
    }
    
    private boolean invoke(String methodName, boolean invoked, boolean start) {
        if (!invoked) {
            transferFile(start);
            return true;
        } else {
            log.warn(methodName + " should only be invoked once.");
            return invoked;
        }
    }
    
    private void transferFile(boolean start) {
        File file = null;
        try {
            file = new File(start ? SwitchableFileSink.START_FILENAME : SwitchableFileSink.STOP_FILENAME);
            file.createNewFile();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        sftp.fileTransfer(file.getName(), watchDir + "/" + file.getName(), SecureFileTransferAgent.FileTransferOption.UPLOAD);
    }
}
