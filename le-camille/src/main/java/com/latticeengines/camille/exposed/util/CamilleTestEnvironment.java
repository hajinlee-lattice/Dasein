package com.latticeengines.camille.exposed.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.CamilleEnvironment.Mode;
import com.latticeengines.camille.exposed.CamilleConfiguration;
import com.netflix.curator.test.TestingServer;

public class CamilleTestEnvironment {
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

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

            CamilleConfiguration config = new CamilleConfiguration();
            config.setConnectionString(server.getConnectString());
            config.setPodId("Development");

            CamilleEnvironment.start(Mode.BOOTSTRAP, config);
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
