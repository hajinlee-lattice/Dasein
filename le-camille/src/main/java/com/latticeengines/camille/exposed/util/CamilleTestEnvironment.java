package com.latticeengines.camille.exposed.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleConfiguration;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.CamilleEnvironment.Mode;
import com.latticeengines.camille.exposed.config.bootstrap.CustomerSpaceServiceBootstrapManager;
import com.latticeengines.camille.exposed.config.bootstrap.ServiceBootstrapManager;
import com.latticeengines.camille.exposed.featureflags.FeatureFlagClient;
import com.latticeengines.camille.exposed.lifecycle.ContractLifecycleManager;
import com.latticeengines.camille.exposed.lifecycle.TenantLifecycleManager;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.camille.exposed.paths.PathConstants;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagDefinition;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
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
    public static synchronized void start() throws Exception {
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

            // set up feature flag definition information
            Map<String, FeatureFlagDefinition> featureFlagMap = getFeatureFlagDefinitions();
            for (String key : featureFlagMap.keySet()) {
                FeatureFlagClient.setDefinition(key, featureFlagMap.get(key));
            }

            // set up products information
            Camille camille = CamilleEnvironment.getCamille();
            Path productsPath = PathBuilder
                    .buildCustomerSpacePath(CamilleEnvironment.getPodId(), getContractId(), getTenantId(), getSpaceId())
                    .append(new Path("/" + PathConstants.SPACECONFIGURATION_NODE + "/" + PathConstants.PRODUCTS_NODE));
            Document products = DocumentUtils.toRawDocument(getDefaultProducts());
            camille.upsert(productsPath, products, ZooDefs.Ids.OPEN_ACL_UNSAFE);

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
        return new CustomerSpaceInfo(new CustomerSpaceProperties(), JsonUtils.serialize(getDefaultFeatureFlags()));
    }

    public static List<String> getDefaultProducts() {
        return Arrays.asList(LatticeProduct.LPA3.getName(), LatticeProduct.PD.getName());
    }

    public static FeatureFlagValueMap getDefaultFeatureFlags() {
        FeatureFlagValueMap featureFlagMap = new FeatureFlagValueMap();
        featureFlagMap.put("flag1", true);
        featureFlagMap.put("flag2", false);
        return featureFlagMap;
    }

    public static Map<String, FeatureFlagDefinition> getFeatureFlagDefinitions() {
        Collection<LatticeProduct> lpi = Collections.singleton(LatticeProduct.LPA3);
        Collection<LatticeProduct> cg = Collections.singleton(LatticeProduct.CG);
        Collection<LatticeProduct> lpiNpd = Arrays.asList(LatticeProduct.LPA3, LatticeProduct.PD);
        Collection<LatticeProduct> lpiPdNcg = Arrays.asList(LatticeProduct.LPA3, LatticeProduct.PD, LatticeProduct.CG);

        Map<String, FeatureFlagDefinition> flagDefinitionMap = new HashMap<String, FeatureFlagDefinition>();
        FeatureFlagDefinition ff1 = new FeatureFlagDefinition();
        ff1.setDisplayName("flag1");
        ff1.setAvailableProducts(new HashSet<LatticeProduct>(lpi));
        flagDefinitionMap.put(ff1.getDisplayName(), ff1);
        FeatureFlagDefinition ff2 = new FeatureFlagDefinition();
        ff2.setDisplayName("flag2");
        ff2.setAvailableProducts(new HashSet<LatticeProduct>(lpiNpd));
        flagDefinitionMap.put(ff2.getDisplayName(), ff2);
        FeatureFlagDefinition ff3 = new FeatureFlagDefinition();
        ff3.setDisplayName("flag3");
        ff3.setAvailableProducts(new HashSet<LatticeProduct>(lpiPdNcg));
        flagDefinitionMap.put(ff3.getDisplayName(), ff3);
        FeatureFlagDefinition ff4 = new FeatureFlagDefinition();
        ff4.setDisplayName("flag4");
        ff4.setAvailableProducts(new HashSet<LatticeProduct>(cg));
        flagDefinitionMap.put(ff4.getDisplayName(), ff4);

        return flagDefinitionMap;
    }

    public static String getDivision() {
        return division;
    }

    public static String getSharedQueues() {
        return sharedQueues;
    }

    public static synchronized void setDivision(String div, String sharedQs) {
        division = div;
        sharedQueues = sharedQs;
        CamilleEnvironment.setDivision(div, sharedQs);
    }

    /**
     * Stops the testing cluster and the camille environment.
     */
    public static synchronized void stop() throws Exception {
        try {
            CamilleEnvironment.stop();
            FeatureFlagClient.teardown();
            if (server != null) {
                server.close();
            }
        } catch (Exception e) {
            log.error("Error stopping Camille test environment", e);
            throw e;
        }
    }
}
