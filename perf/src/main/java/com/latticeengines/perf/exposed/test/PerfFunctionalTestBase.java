package com.latticeengines.perf.exposed.test;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.perf.exposed.metric.sink.SinkCollectionServer;
import com.latticeengines.perf.exposed.metric.sink.SocketSink;


public class PerfFunctionalTestBase {
    private static final Log log = LogFactory.getLog(PerfFunctionalTestBase.class);
    
    private SinkCollectionServer collectionServer = null;
    private Thread collectionServerThread = null;
    
    public PerfFunctionalTestBase(String metricFileName) {
        String hostPortStr = null;
        try {
            Configuration conf = new PropertiesConfiguration("hadoop-metrics2.properties").interpolatedConfiguration();
            hostPortStr = (String) conf.getProperty("ledpjob.sink.file.server");
        } catch (ConfigurationException e) {
            throw new IllegalStateException(e);
        }
        
        if (hostPortStr != null) {
            String[] hostPortTokens = SocketSink.parseServer(hostPortStr);
            collectionServer = new SinkCollectionServer(metricFileName, Integer.parseInt(hostPortTokens[1]));
        }
        
    }

    public void beforeClass() {
        collectionServer.setCanWrite(true);
        collectionServerThread = new Thread(collectionServer);
        collectionServerThread.setDaemon(true);
        collectionServerThread.start();
    }
    
    public void beforeMethod() {
        collectionServer.setCanWrite(true);
    }
    
    public void afterMethod() {
        collectionServer.flushToFile();
        collectionServer.setCanWrite(false);
    }
    
    public void afterClass() {
        collectionServer.setCanWrite(false);
    }
    
    public void flushToFile() {
        collectionServer.flushToFile();
    }

}
