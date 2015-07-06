package com.latticeengines.upgrade.model.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.joda.time.format.DateTimeFormat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.upgrade.UpgradeRunner;
import com.latticeengines.upgrade.domain.BardInfo;
import com.latticeengines.upgrade.jdbc.AuthoritativeDBJdbcManager;
import com.latticeengines.upgrade.jdbc.BardJdbcManager;
import com.latticeengines.upgrade.jdbc.TenantModelJdbcManager;
import com.latticeengines.upgrade.model.service.ModelUpgradeService;
import com.latticeengines.upgrade.yarn.YarnManager;
import com.latticeengines.upgrade.yarn.YarnPathUtils;

@Component("modelUpgrader")
public class ModelUpgradeServiceImpl implements ModelUpgradeService {

    @Autowired
    protected TenantModelJdbcManager tenantModelJdbcManager;

    @Autowired
    protected AuthoritativeDBJdbcManager authoritativeDBJdbcManager;

    @Autowired
    protected BardJdbcManager bardJdbcManager;

    @Autowired
    protected YarnManager yarnManager;

    private static final String BARD_DB = "Bard DB";

    private static final String VISIDB_DL = "VisiDBDL";

    private static final String CUSTOMER_NAME = "CustomerName";

    private static final String[] VERSIONS = new String[]{"1.3.4", "1.4.0"};

    @Value("${dataplatform.customer.basedir}")
    protected String customerBase;

    protected String bardDB;

    protected String instance;

    protected String dlTenantName;

    private static final Joiner commaJoiner = Joiner.on(", ").skipNulls();
    private static final DateTimeFormatter FMT = DateTimeFormat.forPattern("yyyy-MM-dd");

    public void setInfos(List<BardInfo> infos) throws Exception {
        for (BardInfo bardInfo : infos) {
            if (bardInfo.getDisplayName().equals(BARD_DB)) {
                bardDB = bardInfo.getName();
                System.out.println(bardDB);
                instance = bardInfo.getInstance();
                System.out.println("instance: " + instance);
            } else if (bardInfo.getDisplayName().equals(VISIDB_DL)) {
                String settings = bardInfo.getSettings();
                JsonNode parentNode = new ObjectMapper().readTree(settings);
                for (JsonNode node : parentNode) {
                    if (node.get("Key").asText().equals(CUSTOMER_NAME)) {
                        dlTenantName = node.get("Value").asText();
                        System.out.println("tenant name: " + dlTenantName);
                    }
                }
            }
        }
    }

    protected void populateTenantModelInfo() {
        List<String> deploymentIds = new ArrayList<>();
        for (String version : VERSIONS) {
            deploymentIds.addAll(authoritativeDBJdbcManager.getDeploymentIDs(version));
        }
        for (String deploymentId : deploymentIds) {
            List<String> activeModelKeyList = getActiveModelKeyList(deploymentId);
            populateTenantModelInfo(activeModelKeyList);
            System.out.println("_______________________________________");
        }
    }

    private List<String> getActiveModelKeyList(String deploymentId) {
        try {
            List<BardInfo> bardInfos = authoritativeDBJdbcManager.getBardDBInfos(deploymentId);
            setInfos(bardInfos);
            bardJdbcManager.init(bardDB, instance);
            return bardJdbcManager.getActiveModelKey();
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_24001, e);
        }
    }

    private void populateTenantModelInfo(List<String> activeModelKeyList) {
        String modelGuid = "";
        if (activeModelKeyList.size() == 1) {
            modelGuid = StringUtils.remove(activeModelKeyList.get(0), "Model_");
            tenantModelJdbcManager.populateExternalTenantModelInfo(dlTenantName, modelGuid);
        } else if (activeModelKeyList.size() == 0) {
            modelGuid = "No Active Model been found!";
        } else {
            List<String> keys = new ArrayList<>();
            for (String activeModelKey : activeModelKeyList) {
                keys.add(StringUtils.remove(activeModelKey, "Model_"));
            }
            modelGuid = commaJoiner.join(keys);
        }
        tenantModelJdbcManager.populateInternalTenantModelInfo(dlTenantName, modelGuid);
    }

    private void copyCustomerModelsToTupleId(String customer) {
        System.out.print(String.format("Create customer folder %s, if not exists ... ", CustomerSpace.parse(customer)
                .toString()));
        yarnManager.createTupleIdCustomerRootIfNotExist(customer);
        System.out.println("OK");

        System.out.print("Copying model files to the destination folder ... ");
        yarnManager.copyModelsFromSingularToTupleId(customer);
        System.out.println("OK");

        System.out.println("Fix model.json filenames ... ");
        List<String> uuids = yarnManager.findAllUuidsInSingularId(customer);
        for (String uuid: uuids) {
            System.out.print("    " + uuid + " ... ");
            yarnManager.fixModelName(customer, uuid);
            System.out.println("OK");
        }
    }

    private void upgradeModelSummaryForCustomerModels(String customer){
        List<String> uuids = yarnManager.findAllUuidsInSingularId(customer);
        for(String uuid : uuids){
            upgradeModelSummayForCustomerModel(customer, uuid);

            //TODO:song there will be another step to make early 1.4 summaries unclickable
        }
    }

    /**
     * This should happen after copy models from singular to tuple ID path
     * Before the modelsummary.json get downloaded by PLS
     */
    private void upgradeModelSummayForCustomerModel(String customer, String uuid) {
        System.out.print("Check if modelsummary already in tupleId path ...");
        boolean exists = yarnManager.modelSummaryExistsInTupleId(customer, uuid);
        System.out.println(exists ? "YES" : "NO");

        System.out.print("Check if the model is active ...");
        boolean active = tenantModelJdbcManager.modelIsActive(customer, uuid);
        System.out.println(active ? "YES" : "NO");

        boolean toBeGenerated = active;
        if (exists) {
            System.out.print("Check if the modelsummary is complete ...");
            boolean complete = yarnManager.modelSummaryIsCompleteInSingularId(customer, uuid);
            System.out.println(complete ? "YES" : "NO");

            if (!complete) {
                System.out.print("Deleting existing incomplete modelsummary ...");
                yarnManager.deleteModelSummaryInTupleId(customer, uuid);
                System.out.println("OK");
            }

            toBeGenerated = active && !complete;
        }

        if (toBeGenerated) {
            System.out.print("Generating incomplete modelsummary based on model.json ...");
            JsonNode jsonNode = yarnManager.generateModelSummary(customer, uuid);
            System.out.println("OK");

            System.out.print("Uploading modelsummary to tupleId path ...");
            yarnManager.uploadModelsummary(customer, uuid, jsonNode);
            System.out.println("OK");
        }
    }

    private void listTenantModel() {
        System.out.print("Retrieving list of tenants to be upgraded ... ");
        List<String> tenants = tenantModelJdbcManager.getTenantsToUpgrade();
        System.out.println("OK");

        List<String> summaries = new ArrayList<>();
        for (String tenant : tenants) {
            summaries.add(printModelsInTable(tenant));
        }
        for (String summary: summaries) {
            System.out.println(summary);
        }
    }

    private void listTenantModelInHdfs() {
        System.out.print("Retrieving list of tenants to be upgraded ... ");
        List<String> tenants = tenantModelJdbcManager.getTenantsToUpgrade();
        System.out.println("OK");

        List<String> summaries = new ArrayList<>();
        for (String tenant : tenants) {
            summaries.add(printModelsInHdfs(tenant));
        }
        System.out.println("");
        for (String summary: summaries) {
            System.out.println(summary);
        }
    }

    private String printModelsInTable(String customer) {
        List<String> modelGuids = tenantModelJdbcManager.getActiveModels(customer);

        ModelStatistics aggregator = new ModelStatistics();
        for (String modelGuid : modelGuids) {
            String uuid = YarnPathUtils.extractUuid(modelGuid);
            printPreUpgradeStatusOfCustomerModel(customer, uuid, aggregator);
        }

        if (modelGuids.isEmpty()) {
            System.out.println(String.format("\nCustomer %s does not have any active model", customer));
        }

        return String.format("%-30s has %2d models in total, %2d active models, %2d model.json, %2d modelsummary.json.",
                customer, modelGuids.size(), aggregator.activeModels, aggregator.modelJsons, aggregator.modelSummeries);
    }

    private String printModelsInHdfs(String customer) {
        List<String> uuids = yarnManager.findAllUuidsInSingularId(customer);

        ModelStatistics aggregator = new ModelStatistics();
        for (String uuid : uuids) {
            printPreUpgradeStatusOfCustomerModel(customer, uuid, aggregator);
        }

        if (uuids.isEmpty()) {
            System.out.println(String.format("\nCustomer %s does not have any model in hdfs.", customer));
        }

        return String.format("%-30s has %2d models in total, %2d active models, %2d model.json, %2d modelsummary.json.",
                customer, uuids.size(), aggregator.activeModels, aggregator.modelJsons, aggregator.modelSummeries);

    }

    private void printPreUpgradeStatusOfCustomerModel(String customer, String uuid, ModelStatistics statistics) {
        System.out.println(String.format("\n(%s, %s): ", customer, uuid));
        System.out.print("    Model is active? .................... ");
        if (tenantModelJdbcManager.modelIsActive(customer, uuid)) {
            statistics.activeModels++;
            System.out.println("YES");
        } else {
            System.out.println("NO");
        }

        System.out.print("    Model json exists in singular Id? ... ");
        if (yarnManager.modelJsonExistsInSingularId(customer, uuid)) {
            statistics.modelJsons++;
            System.out.println("YES");
        } else {
            System.out.println("NO");
            return;
        }

        System.out.print("    Model was created at ................ ");
        System.out.println(FMT.print(yarnManager.getModelCreationDate(customer, uuid)));

        System.out.print("    Modelsummary already exists? ........ ");
        
        if (yarnManager.modelSummaryExistsInSingularId(customer, uuid)) {
            statistics.modelSummeries++;
            System.out.println("YES");
        } else {
            System.out.println("NO");
        }
    }

    private class ModelStatistics {
        public int activeModels = 0;
        public int modelJsons = 0;
        public int modelSummeries = 0;
    }

    @Override
    public boolean execute(String command, Map<String, Object> parameters) {
        String customer = (String) parameters.get("customer");
        Boolean all = (Boolean) parameters.get("all");

        switch (command) {
        case UpgradeRunner.CMD_LIST:
            if (all) {
                listTenantModelInHdfs();
            } else {
                listTenantModel();
            }
            return true;
        case UpgradeRunner.CMD_MODEL_INFO:
            populateTenantModelInfo();
            return true;
        case UpgradeRunner.CMD_CP_MODELS:
            copyCustomerModelsToTupleId(customer);
            return true;
        case UpgradeRunner.CMD_UPGRADE:
            copyCustomerModelsToTupleId(customer);
            upgradeModelSummaryForCustomerModels(customer);
            return true;
        default:
            return false;
        }
    }
}
