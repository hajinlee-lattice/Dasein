package com.latticeengines.perf.exposed.test;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.latticeengines.perf.exposed.util.SecureFileTransferAgent;


public class PerfFunctionalTestBase {
    private SecureFileTransferAgent sftp = null;
    
    public PerfFunctionalTestBase(String serverAddress, String userId, String password) {
        try {
            Configuration conf = new PropertiesConfiguration("hadoop-metrics2.properties").interpolatedConfiguration();
            
        } catch (ConfigurationException e) {
            throw new IllegalStateException(e);
        }
        sftp = new SecureFileTransferAgent(serverAddress, userId, password);
    }

    public void beforeClass() {
        
    }
    
    public void afterClass() {
        
    }
}
