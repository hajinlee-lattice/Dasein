package com.latticeengines.perf.exposed.test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.perf.exposed.metric.sink.SinkCollectionServer;
import com.latticeengines.perf.exposed.metric.sink.SocketSink;

public class PerfFunctionalTestBase {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(PerfFunctionalTestBase.class);

    private SinkCollectionServer collectionServer = null;
    private ExecutorService exec = null;

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
        exec = Executors.newFixedThreadPool(1, new ThreadFactory() {
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setDaemon(true);
                return t;
            }
        });
        exec.submit(collectionServer);
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
        exec.shutdownNow();
    }

    public void flushToFile() {
        collectionServer.flushToFile();
    }

}
