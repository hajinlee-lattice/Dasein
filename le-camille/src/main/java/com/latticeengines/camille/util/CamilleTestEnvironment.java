package com.latticeengines.camille.util;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.StringReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.camille.CamilleEnvironment;
import com.latticeengines.camille.CamilleEnvironment.Mode;
import com.latticeengines.camille.ConfigJson;
import com.netflix.curator.test.TestingServer;

public class CamilleTestEnvironment {
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    public static final int NUMBER_OF_SERVERS_IN_CLUSTER = 5;

    private static TestingServer server;

    /**
     * Starts a testing cluster and the camille environment.
     */
    public synchronized static void start() throws Exception {
        try {

            if (server != null) {
                server.close();
            }

            server = new TestingServer();

            CamilleEnvironment.stop();

            ConfigJson config = new ConfigJson();
            config.setConnectionString(server.getConnectString());
            config.setPodId("PodID");

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

            if (server != null) {
                server.close();
            }
        } catch (Exception e) {
            log.error("Error stopping Camille test environment", e);
            throw e;
        }
    }
}
