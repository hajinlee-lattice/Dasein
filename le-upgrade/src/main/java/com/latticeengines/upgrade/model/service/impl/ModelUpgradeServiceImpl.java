package com.latticeengines.upgrade.model.service.impl;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.upgrade.UpgradeRunner;
import com.latticeengines.upgrade.jdbc.TenantModelJdbcManager;
import com.latticeengines.upgrade.model.service.ModelUpgradeService;
import com.latticeengines.upgrade.yarn.YarnManager;
import com.latticeengines.upgrade.yarn.YarnPathUtils;

@Component("modelUpgrade")
abstract public class ModelUpgradeServiceImpl implements ModelUpgradeService {

    @Autowired
    protected TenantModelJdbcManager tenantModelJdbcManager;

    @Autowired
    protected YarnManager yarnManager;

    @Value("${upgrade.dao.tenant.model.info.jdbc}")
    protected String tenantModelInfoJDBC;

    @Value("${upgrade.dao.datasource.user}")
    protected String user;

    @Value("${upgrade.dao.datasource.password.encrypted}")
    protected String pass;

    @Value("${dataplatform.customer.basedir}")
    protected String customerBase;

    protected String bardDB;

    protected String instance;

    protected String dlTenantName;

    protected String modelGuid;

    public void upgrade() throws Exception {
    }

    public void setBardDBInfos(List<Map<String, Object>> infosList) throws Exception {
        for (Map<String, Object> infos : infosList) {
            if (infos.get("Display_Name").equals("Bard DB")) {
                bardDB = (String) infos.get("Name");
                System.out.println(bardDB);
                instance = (String) infos.get("Instance");
                System.out.println("instance: " + instance);
            } else if (infos.get("Display_Name").equals("VisiDBDL")) {
                String settings = (String) infos.get("Settings");
                JsonNode parentNode = new ObjectMapper().readTree(settings);
                for (JsonNode node : parentNode) {
                    if (node.get("Key").asText().equals("CustomerName")) {
                        dlTenantName = node.get("Value").asText();
                        System.out.println("tenant name: " + dlTenantName);
                    }
                }
            }
        }
    }



    protected void populateTenantModelInfo() {
        tenantModelJdbcManager.populateTenantModelInfo(dlTenantName, modelGuid);
    }

    private void copyCustomerModelsToTupleId(String customer, String modelGuid) {
        System.out.print(String.format("Create customer folder %s, if not exists ... ", CustomerSpace.parse(customer).toString()));
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

        for (String tenant: tenants) {
            printModelsInTable(tenant);
        }
    }

    private void listTenantModelInHdfs() {
        System.out.print("Retrieving list of tenants to be upgraded ... ");
        List<String> tenants = tenantModelJdbcManager.getTenantsToUpgrade();
        System.out.println("OK");

        for (String tenant: tenants) {
            printModelsInHdfs(tenant);
        }
    }

    private void printModelsInTable(String customer) {
        List<String> modelGuids = tenantModelJdbcManager.getActiveModels(customer);

        for (String modelGuid: modelGuids) {
            printPreUpgradeStatusOfCustomerModel(customer, modelGuid);
        }

        if (modelGuids.isEmpty()) {
            System.out.println(String.format("\nCustomer %s does not have any active model", customer));
        }
    }

    private void printModelsInHdfs(String customer) {
        List<String> uuids = yarnManager.findAllUuidsInSingularId(customer);

        for (String uuid: uuids) {
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
    public void execute(String command, Map<String, Object> parameters) {
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
                break;
            case UpgradeRunner.CMD_CP_MODELS:
                copyCustomerModelsToTupleId(customer, model);
                break;
            default:
                // handled by version specific upgrader
                break;
        }

    };
}
