package com.latticeengines.upgrade.tenant.service.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.upgrade.UpgradeRunner;
import com.latticeengines.upgrade.jdbc.PlsMultiTenantJdbcManager;
import com.latticeengines.upgrade.jdbc.TenantModelJdbcManager;
import com.latticeengines.upgrade.jdbc.UpgradeSummary;
import com.latticeengines.upgrade.model.service.ModelUpgradeService;
import com.latticeengines.upgrade.pls.PlsGaManager;
import com.latticeengines.upgrade.tenant.service.TenantUpgradeService;
import com.latticeengines.upgrade.yarn.YarnManager;

@Component("tenantCleaner")
public class TenantCleanupServiceImpl implements TenantUpgradeService {

    @Autowired
    private PlsGaManager plsGaManager;

    @Autowired
    private PlsMultiTenantJdbcManager plsMultiTenantJdbcManager;

    @Autowired
    private TenantModelJdbcManager tenantModelJdbcManager;

    @Autowired
    private YarnManager yarnManager;

    @Autowired
    private ModelUpgradeService modelUpgrader;

    private static final int MODEL_DOWNLOAD_RETRIES = 10;
    private static Set<String> uuidsInHdfs;
    private static Set<String> uuidsInLp;

    private void getUuids(String customer) {
        uuidsInHdfs = new HashSet<>(yarnManager.findAllUuidsInTupleId(customer));

        uuidsInLp = new HashSet<>();
        for(String uuid: modelUpgrader.getLpUuidsBeforeUpgrade()) {
            if (uuidsInHdfs.contains(uuid)) {
                uuidsInLp.add(uuid);
            }
        }
        System.out.println(String.format("Number of models to be downloaded for customer %s ... ... ... ... %d",
                customer, uuidsInLp.size()));
    }

    private void deleteSingularIdPLSTenant(String customer) {
        System.out.println(String.format("\nThe old customer being deleted from PLS ... ... ... ... %s", customer));

        System.out.print("Deleting singular ID tenant in PLS/GA ... ");
        plsGaManager.deleteTenantWithSingularId(customer);
        System.out.println("OK");
    }

    private void deleteModelSummariesForNewTenant(String customer) {
        System.out.println(String.format("\nRemoving modelsummaries in the new tenant for ... ... ... ... %s", customer));

        System.out.print("Removing modelsummaries in PLS_MultiTenant DB ... ");
        String tupleId = CustomerSpace.parse(customer).toString();
        plsMultiTenantJdbcManager.deleteModelSummariesByTenantId(tupleId);
        System.out.println("OK");
    }

    private void waitForModelDownloadedAndRename(String customer) {
        System.out.println("  Waiting for all models for customer " + customer + " are downloaded ... ");
        Set<String> uuidsDownloaded = new HashSet<>();
        for (String uuid: uuidsToBeDownloaded(customer)) {
            if (uuidsInHdfs.contains(uuid)) {
                System.out.println("    " + uuid + " ... ");
                String modelGuid = modelIsDownloaded(uuid);
                System.out.println(modelGuid == null ? "NO" : "YES. ModelGuid = " + modelGuid);
                if (modelGuid != null) {
                    uuidsDownloaded.add(modelGuid);
                }
            }
        }

        System.out.println("  Updating model names for customer " + customer + ", if necessary ... ");
        Map<String, String> modelNames = modelUpgrader.getUuidModelNameMap();
        for (Map.Entry<String, String> entry: modelNames.entrySet()) {
            if (uuidsDownloaded.contains(entry.getKey())) {
                updateModelName(entry.getKey(), entry.getValue());
            }
        }

    }

    private void populateUpgradeSummary(String customer) {
        System.out.print("Populate upgrade summary in ModelUpgrade DB ... ");

        UpgradeSummary summary = new UpgradeSummary();
        summary.tenantName = customer;

        summary.modelsInHdfs = uuidsInHdfs.size();
        int summaries = 0;
        for (String uuid: uuidsInHdfs) {
            if (yarnManager.modelSummaryExistsInTupleId(customer, uuid)) {
                summaries++;
            }
        }
        summary.modelsummariesInHdfs = summaries;

        Set<String> modelIdsInLp = plsMultiTenantJdbcManager.getModelGuidsForCustomer(customer);
        summary.modelsInLp = modelIdsInLp.size();

        for (String modelGuid: modelIdsInLp) {
            if (plsMultiTenantJdbcManager.isActive(modelGuid)) {
                summary.activeModelGuid = modelGuid;
                break;
            }
        }
        tenantModelJdbcManager.populateUpgradeSummary(summary);

        System.out.println("OK");
    }

    private void updateModelName(String uuid, String name) {
        System.out.print("  Updating the name of model " + uuid + " to " + name + " ... ");
        String modelGuid = modelIsDownloaded(uuid);
        plsGaManager.updateModelName(modelGuid, name);
        System.out.println("OK");
    }

    private String modelIsDownloaded(String uuid) {
        int retry = 0;
        while (retry < MODEL_DOWNLOAD_RETRIES) {
            String guid = plsMultiTenantJdbcManager.findModelGuidByUuid(uuid);
            if (guid != null) return guid;

            try {
                Thread.sleep(3000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            retry++;
        }
        return plsMultiTenantJdbcManager.findModelGuidByUuid(uuid);
    }

    private void updateModelsActivity(String customer) {
        System.out.println(String.format("Updating models activity status for ... ... ... ... %s", customer));

        List<String> modelIds =plsGaManager.getModelIds(customer);
        for (String modelId: modelIds) {
            if (tenantModelJdbcManager.modelShouldBeActive(modelId)) {
                System.out.print(String.format("  Setting model %s to ACTIVE ... ", modelId));
                plsGaManager.setModelActivity(modelId, true);
                System.out.println("OK");
            }
        }
    }

    private List<String> uuidsToBeDownloaded(String customer) {
        List<String> activeUuids = tenantModelJdbcManager.getActiveUuids(customer);
        activeUuids.addAll(uuidsInLp);
        return activeUuids;
    }

    private void upgrade(String customer) {
        getUuids(customer);
        deleteSingularIdPLSTenant(customer);
        deleteModelSummariesForNewTenant(customer);
        waitForModelDownloadedAndRename(customer);
        updateModelsActivity(customer);
        populateUpgradeSummary(customer);
    }

    @Override
    public boolean execute(String command, Map<String, Object> parameters) {
        String customer = (String) parameters.get("customer");

        switch (command) {
            case UpgradeRunner.CMD_UPGRADE:
                upgrade(customer);
                return true;
            default:
                return false;
        }

    }

}
