package com.latticeengines.camille;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.TimeLimiter;
import com.latticeengines.camille.CamilleEnvironment.Mode;
import com.netflix.curator.test.TestingCluster;

public class CamilleTestEnvironment {
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    public static final int NUMBER_OF_SERVERS_IN_CLUSTER = 5;

    private static TestingCluster cluster;

    /**
     * Starts a testing cluster and the camille environment.
     */
    public synchronized static void start() throws Exception {
        try {

            if (cluster != null) {
                cluster.close();
            }

            cluster = new TestingCluster(NUMBER_OF_SERVERS_IN_CLUSTER);
            cluster.start();

            CamilleEnvironment.stop();

            ConfigJson config = new ConfigJson();
            config.setConnectionString(cluster.getConnectString());
            config.setPodId("ignored");

            OutputStream stream = new ByteArrayOutputStream();

            new ObjectMapper().writeValue(stream, config);

            CamilleEnvironment.start(Mode.BOOTSTRAP, new StringReader(stream.toString()));
        } catch (Exception e) {
            log.error("Error starting Camille environment", e);
            throw e;
        }
    }

    /**
     * Stops the testing cluster and the camille environment.
     */
    public synchronized static void stop() throws Exception {
        try {
            CamilleEnvironment.stop();

            if (cluster != null) {
                cluster.close();
            }
        } catch (Exception e) {
            log.error("Error stopping Camille test environment", e);
            throw e;
        }
    }
}
