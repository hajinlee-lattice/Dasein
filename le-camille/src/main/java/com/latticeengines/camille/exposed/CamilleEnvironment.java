package com.latticeengines.camille.exposed;

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.camille.exposed.lifecycle.PodLifecycleManager;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.lifecycle.PodInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.PodProperties;

public final class CamilleEnvironment {

    protected CamilleEnvironment() {
        throw new UnsupportedOperationException();
    }
    public enum Mode {
        BOOTSTRAP, RUNTIME
    };

    // these are reasonable arguments for the ExponentialBackoffRetry. The first
    // retry will wait 1 second - the second will wait up to 2 seconds - the
    // third will wait up to 4 seconds.
    private static final ExponentialBackoffRetry RETRY_POLICY = new ExponentialBackoffRetry(1000, 3);
    private static final int CONNECTION_WAIT_TIME = 1;
    private static final TimeUnit CONNECTION_WAIT_TIME_UNITS = TimeUnit.MINUTES;

    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

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

        CuratorFramework client = CuratorFrameworkFactory.newClient(camilleConfig.getConnectionString(), RETRY_POLICY);
        client.start();
        boolean connected;
        try {
            connected = client.blockUntilConnected(CONNECTION_WAIT_TIME, CONNECTION_WAIT_TIME_UNITS);
        } catch (InterruptedException ie) {
            log.error(String.format(
                    "Interrupted waiting for connection to Zookeeper (connectionString=%s) to be established.",
                    config.getConnectionString()), ie);
            stopNoSync();
            throw ie;
        }

        if (!connected) {
            stopNoSync();
            throw new RuntimeException(String.format(
                    "Timed out connecting to Zookeeper (connectionString=%s) after %s %s", config.getConnectionString(),
                    CONNECTION_WAIT_TIME, CONNECTION_WAIT_TIME_UNITS.toString().toLowerCase()));
        }

        camille = new Camille(client);

        switch (mode) {
        case BOOTSTRAP:
            PodLifecycleManager.create(camilleConfig.getPodId(), getAutogenPodInfo());
            break;
        case RUNTIME:
            if (!PodLifecycleManager.exists(camilleConfig.getPodId())) {
                Exception e = new RuntimeException(
                        String.format("Runtime mode requires an existing pod with Id=%s", camilleConfig.getPodId()));
                log.error(e.getMessage(), e);
                stopNoSync();
                throw e;
            }
            break;
        default:
        }
        log.info("Camille env started pod " + camilleConfig.getPodId() + ", conn " + camilleConfig.getConnectionString()
                + ", div " + camilleConfig.getDivision() + ", shareQ " + camilleConfig.getSharedQueues());

    }

    private static PodInfo getAutogenPodInfo() {
        return new PodInfo(new PodProperties(String.format("Pod %s", getPodId()),
                String.format("Autogenerated by Camille for pod %s", getPodId())));
    }

    public static PodInfo getPodInfo() throws Exception {
        return PodLifecycleManager.getInfo(getPodId());
    }

    public static synchronized String getPodId() {
        if (camilleConfig == null) {
            throw new IllegalStateException("CamilleEnvironment has not been started");
        }
        return camilleConfig.getPodId();
    }

    public static synchronized String getDivision() {
        if (camilleConfig == null) {
            throw new IllegalStateException("CamilleEnvironment has not been started");
        }
        return camilleConfig.getDivision();
    }

    public static synchronized void setDivision(String division, String sharedQueues) {
        if (camilleConfig == null) {
            throw new IllegalStateException("CamilleEnvironment has not been started");
        }
        camilleConfig.setDivision(division);
        camilleConfig.setSharedQueues(sharedQueues);
    }

    public static synchronized boolean isSharedQueue(String queueName) {
        if (camilleConfig == null) {
            throw new IllegalStateException("CamilleEnvironment has not been started");
        }
        return camilleConfig.isSharedQueue(queueName);
    }

    public static boolean isStarted() {
        return  (camille != null && camille.getCuratorClient() != null
                && camille.getCuratorClient().getState().equals(CuratorFrameworkState.STARTED));
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


    // Paths
    public static Path getFabricEntityPath(String entityName) {
        return PathBuilder.buildFabricEntityPath(getPodId(), entityName);
    }

}
