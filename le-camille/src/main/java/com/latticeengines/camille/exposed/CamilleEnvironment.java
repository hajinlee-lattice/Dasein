package com.latticeengines.camille.exposed;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.camille.exposed.lifecycle.PodLifecycleManager;

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
    private static CamilleConfiguration camilleConfig = null;

    public static synchronized void start(Mode mode, CamilleConfiguration config) throws Exception {
        if (camille != null && camille.getCuratorClient() != null
                && camille.getCuratorClient().getState().equals(CuratorFrameworkState.STARTED)) {
            return;
        }

        if (mode == null) {
            IllegalArgumentException e = new IllegalArgumentException("mode cannot be null");
            log.error(e.getMessage(), e);
            stopNoSync();
            throw e;
        }

        camilleConfig = config;

        CuratorFramework client = CuratorFrameworkFactory.newClient(camilleConfig.getConnectionString(), retryPolicy);
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
            PodLifecycleManager.create(camilleConfig.getPodId());
            break;
        case RUNTIME:
            if (!PodLifecycleManager.exists(camilleConfig.getPodId())) {
                Exception e = new RuntimeException(String.format("Runtime mode requires an existing pod with Id=%s",
                        camilleConfig.getPodId()));
                log.error(e.getMessage(), e);
                stopNoSync();
                throw e;
            }
            break;
        }
    }

    public synchronized static String getPodId() {
        if (camilleConfig == null) {
            throw new IllegalStateException("CamilleEnvironment has not been started");
        }
        return camilleConfig.getPodId();
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
            camilleConfig = null;
        }
    }

    public static synchronized void stop() {
        stopNoSync();
    }

    public static Camille getCamille() {
        return camille;
    }
}
