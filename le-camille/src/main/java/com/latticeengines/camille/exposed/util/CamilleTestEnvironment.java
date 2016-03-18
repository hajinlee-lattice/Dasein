package com.latticeengines.camille.exposed.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.camille.exposed.CamilleConfiguration;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.CamilleEnvironment.Mode;
import com.latticeengines.camille.exposed.config.bootstrap.CustomerSpaceServiceBootstrapManager;
import com.latticeengines.camille.exposed.config.bootstrap.ServiceBootstrapManager;
import com.latticeengines.camille.exposed.lifecycle.ContractLifecycleManager;
import com.latticeengines.camille.exposed.lifecycle.TenantLifecycleManager;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantProperties;
import com.netflix.curator.test.TestingServer;

public class CamilleTestEnvironment {
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    private static TestingServer server;
    private static String division = null;
    private static String sharedQueues = null;

    /**
     * Starts a testing cluster and the camille environment.
     */
    public synchronized static void start() throws Exception {
        try {

            if (server != null) {
                server.close();
            }

            log.info("Starting Camille environment");

            server = new TestingServer();

            CamilleEnvironment.stop();

            CamilleConfiguration config = new CamilleConfiguration();
            config.setConnectionString(server.getConnectString());
            config.setPodId(getPodId());
            config.setDivision("");
            config.setSharedQueues("");
            division = null;
            sharedQueues = null;

            CamilleEnvironment.start(Mode.BOOTSTRAP, config);

            ContractLifecycleManager.create(getContractId(), getContractInfo());
            TenantLifecycleManager.create(getContractId(), getTenantId(), getTenantInfo(), getSpaceId(),
                    getCustomerSpaceInfo());

            CustomerSpaceServiceBootstrapManager.resetAll();
            ServiceBootstrapManager.resetAll();
        } catch (Exception e) {
            log.error("Error starting Camille environment", e);
            throw e;
        }
    }

    public static String getPodId() {
        return "Development";
    }

    public static String getContractId() {
        return "Widgettech";
    }

    public static String getTenantId() {
        return "Widgettech";
    }

    public static String getSpaceId() {
        return "Development";
    }

    public static CustomerSpace getCustomerSpace() {
        return new CustomerSpace(getContractId(), getTenantId(), getSpaceId());
    }

    public static ContractInfo getContractInfo() {
        return new ContractInfo(new ContractProperties());
    }

    public static TenantInfo getTenantInfo() {
        return new TenantInfo(new TenantProperties());
    }

    public static CustomerSpaceInfo getCustomerSpaceInfo() {
        return new CustomerSpaceInfo(new CustomerSpaceProperties(), "");
    }

    public static String getDivision() {
        return division;
    }

    public static String getSharedQueues() {
        return sharedQueues;
    }

    public synchronized static void setDivision(String div, String sharedQs) {
        division = div;
        sharedQueues = sharedQs;
        CamilleEnvironment.setDivision(div, sharedQs);
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
