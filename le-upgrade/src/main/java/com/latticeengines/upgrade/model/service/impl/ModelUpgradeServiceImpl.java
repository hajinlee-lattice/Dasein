package com.latticeengines.upgrade.model.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

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

@Component("modelUpgrade")
public abstract class ModelUpgradeServiceImpl implements ModelUpgradeService {

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

    @Value("${dataplatform.customer.basedir}")
    protected String customerBase;

    protected String bardDB;

    protected String instance;

    protected String dlTenantName;

    protected String version;

    private static final Joiner commaJoiner = Joiner.on(", ").skipNulls();

    public abstract void upgrade() throws Exception;

    @Override
    public void setVersion(String version) {
        this.version = version;
    }

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
        List<String> deploymentIds = authoritativeDBJdbcManager.getDeploymentIDs(version);
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
        if (activeModelKeyList.size() == 1) {
            String modelGuid = StringUtils.remove(activeModelKeyList.get(0), "Model_");
            tenantModelJdbcManager.populateExternalTenantModelInfo(dlTenantName, modelGuid);
        } else if (activeModelKeyList.size() == 0) {
            tenantModelJdbcManager.populateInternalTenantModelInfo(dlTenantName, "No Active Model been found!");
        } else {
            List<String> keys = new ArrayList<>();
            for (String activeModelKey : activeModelKeyList) {
                keys.add(StringUtils.remove(activeModelKey, "Model_"));
            }
            tenantModelJdbcManager.populateInternalTenantModelInfo(dlTenantName, commaJoiner.join(keys));
        }
    }

    private void copyCustomerModelsToTupleId(String customer, String modelGuid) {
        System.out.print(String.format("Create customer folder %s, if not exists ... ", CustomerSpace.parse(customer)
                .toString()));
        yarnManager.createTupleIdCustomerRootIfNotExist(customer);
        System.out.println("OK");

        System.out.print("Copying model files to the destination folder ... ");
        yarnManager.copyModelsFromSingularToTupleId(customer);
        System.out.println("OK");

        System.out.print("Fix model.json filenames ... ");
        yarnManager.fixModelName(customer, modelGuid);
        System.out.println("OK");
    }

    private void listTenantModel() {
        System.out.print("Retrieving list of tenants to be upgraded ... ");
        List<String> tenants = tenantModelJdbcManager.getTenantsToUpgrade();
        System.out.println("OK");

        for (String tenant : tenants) {
            printModelsInTable(tenant);
        }
    }

    private void listTenantModelInHdfs() {
        System.out.print("Retrieving list of tenants to be upgraded ... ");
        List<String> tenants = tenantModelJdbcManager.getTenantsToUpgrade();
        System.out.println("OK");

        for (String tenant : tenants) {
            printModelsInHdfs(tenant);
        }
    }

    private void printModelsInTable(String customer) {
        List<String> modelGuids = tenantModelJdbcManager.getActiveModels(customer);

        for (String modelGuid : modelGuids) {
            printPreUpgradeStatusOfCustomerModel(customer, modelGuid);
        }

        if (modelGuids.isEmpty()) {
            System.out.println(String.format("\nCustomer %s does not have any active model", customer));
        }
    }

    private void printModelsInHdfs(String customer) {
        List<String> uuids = yarnManager.findAllUuidsInSingularId(customer);

        for (String uuid : uuids) {
            String modelGuid = YarnPathUtils.constructModelGuidFromUuid(uuid);
            printPreUpgradeStatusOfCustomerModel(customer, modelGuid);
        }

        if (uuids.isEmpty()) {
            System.out.println(String.format("\nCustomer %s does not have any model in hdfs.", customer));
        }
    }

    private void printPreUpgradeStatusOfCustomerModel(String customer, String modelGuid) {
        System.out.println(String.format("\n(%s, %s): ", customer, modelGuid));

        System.out.print("    Model is active? .................... ");
        if (tenantModelJdbcManager.modelIsActive(customer, modelGuid)) {
            System.out.println("YES");
        } else {
            System.out.println("NO");
        }

        System.out.print("    Model json exists in singular Id? ... ");
        if (yarnManager.modelJsonExistsInSingularId(customer, modelGuid)) {
            System.out.println("YES");
        } else {
            System.out.println("NO");
            return;
        }

        System.out.print("    Modelsummary already exists? ........ ");
        if (yarnManager.modelSummaryExistsInSingularId(customer, modelGuid)) {
            System.out.println("YES");
        } else {
            System.out.println("NO");
        }
    }

    @Override
    public boolean execute(String command, Map<String, Object> parameters) {
        String customer = (String) parameters.get("customer");
        String model = (String) parameters.get("model");
        Boolean all = (Boolean) parameters.get("all");

        switch (command) {
        case "list":
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
            copyCustomerModelsToTupleId(customer, model);
            return true;
        default:
            // handled by version specific upgrader
            return false;
        }

    }
}
