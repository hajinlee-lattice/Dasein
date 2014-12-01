package com.latticeengines.camille;

import java.io.IOException;
import java.io.Reader;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.camille.lifecycle.PodLifecycleManager;

public class CamilleEnvironment {
    public enum Mode {
        BOOTSTRAP, RUNTIME
    };

    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    // these are reasonable arguments for the ExponentialBackoffRetry. The first
    // retry will wait 1 second - the second will wait up to 2 seconds - the
    // third will wait up to 4 seconds.
    private static final ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);

    // singleton instance
    private static Camille camille = null;
    private static ConfigJson config = null;

    public static synchronized void start(Mode mode, Reader configJsonReader) throws Exception {
        if (camille != null && camille.getCuratorClient() != null
                && camille.getCuratorClient().getState().equals(CuratorFrameworkState.STARTED)) {

            IllegalStateException ise = new IllegalStateException("Camille environment is already started");
            log.error(ise.getMessage(), ise);
            throw ise;
        }

        if (mode == null) {
            IllegalArgumentException e = new IllegalArgumentException("mode cannot be null");
            log.error(e.getMessage(), e);
            stopNoSync();
            throw e;
        }

        config = null;
        try {
            config = new ObjectMapper().readValue(configJsonReader, ConfigJson.class);
        } catch (IOException ioe) {
            log.error("An error occurred reading the configuration file.", ioe);
            stopNoSync();
            throw ioe;
        }

        CuratorFramework client = CuratorFrameworkFactory.newClient(config.getConnectionString(), retryPolicy);
        client.start();
        try {
            client.blockUntilConnected();
        } catch (InterruptedException ie) {
            log.error("Waiting for Curator connection was interrupted.", ie);
            stopNoSync();
            throw ie;
        }

        camille = new Camille(client);

        switch (mode) {
        case BOOTSTRAP:
            PodLifecycleManager.create(config.getPodId());
            break;
        case RUNTIME:
            if (!PodLifecycleManager.exists(config.getPodId())) {
                Exception e = new RuntimeException(String.format("Runtime mode requires an existing pod with Id=%s",
                        config.getPodId()));
                log.error(e.getMessage(), e);
                stopNoSync();
                throw e;
            }
            break;
        }
    }

    public synchronized static String getPodId() {
        if (config == null) {
            throw new IllegalStateException("CamilleEnvironment has not been started");
        }
        return config.getPodId();
    }

    /**
     * Does a stop but does not synchronize on the class object. Only call this
     * from a static synchronized method.
     */
    private static void stopNoSync() {
        if (camille != null && camille.getCuratorClient() != null
                && camille.getCuratorClient().getState().equals(CuratorFrameworkState.STARTED)) {
            camille.getCuratorClient().close();
            camille = null;
            config = null;
        }
    }

    public static synchronized void stop() {
        stopNoSync();
    }

    public static Camille getCamille() {
        return camille;
    }
}
