package com.latticeengines.upgrade.model.service.impl;

import java.util.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.upgrade.UpgradeRunner;
import com.latticeengines.upgrade.domain.BardInfo;
import com.latticeengines.upgrade.jdbc.AuthoritativeDBJdbcManager;
import com.latticeengines.upgrade.jdbc.BardJdbcManager;
import com.latticeengines.upgrade.jdbc.PlsMultiTenantJdbcManager;
import com.latticeengines.upgrade.jdbc.TenantModelJdbcManager;
import com.latticeengines.upgrade.model.decrypt.ModelDecryptor;
import com.latticeengines.upgrade.model.service.ModelUpgradeService;
import com.latticeengines.upgrade.yarn.YarnManager;
import com.latticeengines.upgrade.yarn.YarnPathUtils;

@Component("modelUpgrader")
public class ModelUpgradeServiceImpl implements ModelUpgradeService {

    @Autowired
    private TenantModelJdbcManager tenantModelJdbcManager;

    @Autowired
    private AuthoritativeDBJdbcManager authoritativeDBJdbcManager;

    @Autowired
    private BardJdbcManager bardJdbcManager;

    @Autowired
    private PlsMultiTenantJdbcManager plsMultiTenantJdbcManager;

    @Autowired
    private YarnManager yarnManager;

    private static final String BARD_DB = "Bard DB";

    private static final String VISIDB_DL = "VisiDBDL";

    private static final String CUSTOMER_NAME = "CustomerName";

    private static final String[] VERSIONS = new String[]{"1.3.4", "1.4.0"};

    @Value("${dataplatform.customer.basedir}")
    private String customerBase;

    private String bardDB;

    private String instance;

    private String dlTenantName;

    @Autowired
    private Configuration yarnConfiguration;

    private static final Joiner commaJoiner = Joiner.on(", ").skipNulls();
    private static final DateTimeFormatter FMT = DateTimeFormat.forPattern("yyyy-MM-dd");
    private static final Set<String> originalUuids = new HashSet<>();

    @Override
    public Set<String> getLpUuidsBeforeUpgrade() {
        return originalUuids;
    }

    public void setInfos(List<BardInfo> infos) throws Exception {
        dlTenantName = "Unknown";
        for (BardInfo bardInfo : infos) {
            if (bardInfo.getDisplayName().endsWith(BARD_DB)) {
                bardDB = bardInfo.getName();
                System.out.println("BardDB: " + bardDB);
                instance = bardInfo.getInstance();
                System.out.println("instance: " + instance);
            } else if (bardInfo.getDisplayName().endsWith(VISIDB_DL)) {
                String settings = bardInfo.getSettings();
                JsonNode parentNode = new ObjectMapper().readTree(settings);
                for (JsonNode node : parentNode) {
                    if (node.get("Key").asText().equals(CUSTOMER_NAME)) {
                        dlTenantName = node.get("Value").asText();
                    }
                }
            }
        }
        System.out.println("tenant name: " + dlTenantName);
    }

    protected void populateTenantModelInfo() {
        List<String> deploymentIds = new ArrayList<>();
        for (String version : VERSIONS) {
            deploymentIds.addAll(authoritativeDBJdbcManager.getDeploymentIDs(version));
        }
        for (String deploymentId : deploymentIds) {
            System.out.println("_______________________________________");
            try {
                List<String> activeModelKeyList = getActiveModelKeyList(deploymentId);
                populateTenantModelInfo(activeModelKeyList);
            } catch (Exception e) {
                System.out.println("Error: ");
                e.printStackTrace();
            }
            System.out.println("_______________________________________");
        }

        System.out.print("Removing duplicated TenantModel information ... ");
        tenantModelJdbcManager.removeDuplicatedTenantModelInfo();
        System.out.println("OK");
    }

    private List<String> getActiveModelKeyList(String deploymentId){
        try {
            List<BardInfo> bardInfos = authoritativeDBJdbcManager.getBardDBInfos(deploymentId);
            setInfos(bardInfos);
            bardJdbcManager.init(bardDB, instance);
            return bardJdbcManager.getActiveModelKey();
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_24001, e);
        }
    }

    private Set<String> getModelKeyList(String deploymentId) {
        try {
            Set<String> modelKeys = new HashSet<>();
            modelKeys.addAll(bardJdbcManager.getActiveModelKey());
            modelKeys.addAll(bardJdbcManager.getModelGuidsWithinLast2Weeks());
            return modelKeys;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_24001, e);
        }
    }

    public void exportModelsFromBardToHdfs(String customer){
        System.out.print(String.format("Create customer folder %s, if not exists ... ", CustomerSpace.parse(customer)
                .toString()));
        yarnManager.createTupleIdCustomerRootIfNotExist(customer);
        System.out.println("OK");

        Set<String> modelKeyList = getModelKeyList(customer);
        for(String modelKey : modelKeyList){
            String modelContent;
            try {
                modelContent = ModelDecryptor.decrypt(bardJdbcManager.getModelContent(modelKey));
            } catch (Exception e){
                throw new LedpException(LedpCode.LEDP_24000, "Cannot decrypt the file", e);
            }

            String uuid = YarnPathUtils.extractUuid(modelKey);
            String modelPath;
            try{
                modelPath = yarnManager.findModelPathInSingular(customer, uuid);
                System.out.println("Find the model " + modelPath + " in singular id. About to write to tuple id folder");
                HdfsUtils.writeToFile(yarnConfiguration, YarnPathUtils.substituteByTupleId(modelPath), modelContent);
            }catch(LedpException e){
                System.out.println("Cannot find such model in singular id; now try to search for tuple id.");
                modelPath = yarnManager.findModelPathInTuple(customer, uuid);
                System.out.println("Find the model " + modelPath + " in tuple id. About to write the content");
                try {
                    HdfsUtils.writeToFile(yarnConfiguration, modelPath, modelContent);
                } catch (Exception e1) {
                    throw new LedpException(LedpCode.LEDP_24000, "Failed to copy file from one src to dest path.", e1);
                }
            }catch(Exception e){
                throw new LedpException(LedpCode.LEDP_24000, "Failed to copy file from one src to dest path.", e);
            }
            yarnManager.fixModelNameInTupleId(customer, uuid);
        }
    }

    private void populateTenantModelInfo(List<String> activeModelKeyList) {
        String modelGuid;
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

//    private void copyCustomerModelsToTupleId(String customer) {
//        System.out.print(String.format("Create customer folder %s, if not exists ... ", CustomerSpace.parse(customer)
//                .toString()));
//        yarnManager.createTupleIdCustomerRootIfNotExist(customer);
//        System.out.println("OK");
//
//        System.out.print("Moving models from singular to tuple ID ... ");
//        int nModels = yarnManager.moveModelsFromSingularToTupleId(customer);
//        System.out.println(String.format("OK. %02d models have been moved.", nModels));
//
//        System.out.println("Fix model.json filenames ... ");
//        List<String> uuids = yarnManager.findAllUuidsInTupleId(customer);
//        for (String uuid: uuids) {
//            System.out.print("    " + uuid + " ... ");
//            yarnManager.fixModelNameInTupleId(customer, uuid);
//            System.out.println("OK");
//        }
//    }

    private void upgradeModelSummaryForCustomerModels(String customer){
        List<String> uuids = yarnManager.findAllUuidsInSingularId(customer);
        for(String uuid : uuids){
            upgradeModelSummayForCustomerModel(customer, uuid);
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

        System.out.print("Check if the modelsummary was generated in 1.4 ...");
        boolean in1_4 = plsMultiTenantJdbcManager.hasUuid(uuid);
        System.out.println(in1_4 ? "YES" : "NO");

        if (in1_4) {
            originalUuids.add(uuid);
            //System.out.print("Copying the whole model folder ...");
            //yarnManager.moveModelFolderFromSingularToTupleId(customer, uuid);
            //System.out.println("OK");
        }

        boolean toBeDeleted = exists && !in1_4;
        boolean toBeGenerated = active && !in1_4;

        if (toBeDeleted) {
            System.out.print("Deleting existing incomplete modelsummary ...");
            yarnManager.deleteModelSummaryInTupleId(customer, uuid);
            System.out.println("OK");
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

    private void listTenantModelInHdfsForCustomer(String customer) {
        String summary = printModelsInHdfs(customer);
        System.out.println("");
        System.out.println(summary);
    }

    private String printModelsInHdfs(String customer) {
        List<String> uuids = yarnManager.findAllUuidsInSingularId(customer);

        ModelStatistics aggregator = new ModelStatistics();
        for (String uuid : uuids) {
            printPreUpgradeStatusOfCustomerModel(customer, uuid, aggregator);
        }

        int modlesInTupleId = yarnManager.findAllUuidsInTupleId(customer).size();

        if (uuids.size() + modlesInTupleId == 0) {
            System.out.println(String.format("\nCustomer %s does not have any model in hdfs.", customer));
        }

        return String.format("%-30s has %2d models in total, %2d active models, %2d model.json, " +
                        "%2d modelsummary.json, %2d of them already in PLS 1.4 DB, " +
                        "%2d models already in TupleID folder.",
                customer, uuids.size(), aggregator.activeModels, aggregator.modelJsons,
                aggregator.modelSummeries, aggregator.summariesIn1_4, modlesInTupleId);

    }

    private void printPreUpgradeStatusOfCustomerModel(String customer, String uuid, ModelStatistics statistics) {
        System.out.println(String.format("\n(%s, %s): ", customer, uuid));
        System.out.print("    Model is active? .................... ");
        boolean active = tenantModelJdbcManager.modelIsActive(customer, uuid);
        if (active) {
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

        System.out.print("    Modelsummary in on PLS 1.4 DB? ...... ");
        boolean in1_4 = plsMultiTenantJdbcManager.hasUuid(uuid);
        if (in1_4) {
            statistics.summariesIn1_4++;
            System.out.println("YES");
        } else {
            System.out.println("NO");
        }

        if (!active && !in1_4) return;

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

    private void upgrade(String customer) {
        exportModelsFromBardToHdfs(customer);
        upgradeModelSummaryForCustomerModels(customer);
    }

    private class ModelStatistics {
        public int activeModels = 0;
        public int modelJsons = 0;
        public int modelSummeries = 0;
        public int summariesIn1_4 = 0;
    }

    @Override
    public boolean execute(String command, Map<String, Object> parameters) {
        String customer = (String) parameters.get("customer");
        Boolean all = (Boolean) parameters.get("all");

        try {
            List<BardInfo> bardInfos = authoritativeDBJdbcManager.getBardDBInfos(customer);
            setInfos(bardInfos);
            bardJdbcManager.init(bardDB, instance);
        } catch (Exception e2) {
            throw new LedpException(LedpCode.LEDP_24000, e2);
        }

        switch (command) {
        case UpgradeRunner.CMD_LIST:
            if (all) {
                listTenantModelInHdfs();
            } else {
                listTenantModelInHdfsForCustomer(customer);
            }
            return true;
        case UpgradeRunner.CMD_MODEL_INFO:
            populateTenantModelInfo();
            return true;
        case UpgradeRunner.CMD_CP_MODELS:
            exportModelsFromBardToHdfs(customer);
            return true;
        case UpgradeRunner.CMD_UPGRADE:
            upgrade(customer);
            return true;
        default:
            return false;
        }
    }
}
