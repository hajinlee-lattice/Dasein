package com.latticeengines.cdl.workflow.steps.migrate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ImportMigrateReport;
import com.latticeengines.domain.exposed.cdl.ImportMigrateTracking;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.EntityMatchMigrateStepConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;
import com.latticeengines.proxy.exposed.cdl.MigrateTrackingProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("startMigrate")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class StartMigrate extends BaseWorkflowStep<EntityMatchMigrateStepConfiguration> {

    private static final String DEFAULT_SYSTEM = "DefaultSystem";

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private MigrateTrackingProxy migrateTrackingProxy;

    @Inject
    private CDLProxy cdlProxy;

    @Inject
    private DropBoxProxy dropBoxProxy;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Override
    public void execute() {
        Long migrateTrackingPid = configuration.getMigrateTrackingPid();
        CustomerSpace customerSpace = configuration.getCustomerSpace();
        putObjectInContext(CDL_ACTIVE_VERSION, dataCollectionProxy.getActiveVersion(customerSpace.toString()));
        Map<BusinessEntity, List<String>> dataFeedTaskMap = configuration.getDataFeedTaskMap();
        if (MapUtils.isEmpty(dataFeedTaskMap)) {
            throw new RuntimeException("No import template to be migrated!");
        }
        // update migrate tracking record
        migrateTrackingProxy.updateStatus(customerSpace.toString(), migrateTrackingPid, ImportMigrateTracking.Status.MIGRATING);
        List<String> uniqueIds = new ArrayList<>();
        dataFeedTaskMap.forEach((key, value) -> uniqueIds.addAll(value));
        List<DataFeedTask> dataFeedTasks = dataFeedProxy.getDataFeedTaskByUniqueIds(customerSpace.toString(),
                uniqueIds);
        ImportMigrateReport report = new ImportMigrateReport();
        List<String> accountTemplates = new ArrayList<>();
        List<String> contactTemplates = new ArrayList<>();
        List<String> transactionTemplates = new ArrayList<>();
        List<ImportMigrateReport.BackupInfo> backupInfoList = new ArrayList<>();
        dataFeedTasks.forEach(dataFeedTask -> {
            String backupName = cdlProxy.backupTemplate(customerSpace.toString(), dataFeedTask.getUniqueId());
            ImportMigrateReport.BackupInfo backupInfo = new ImportMigrateReport.BackupInfo();
            backupInfo.setTaskId(dataFeedTask.getUniqueId());
            backupInfo.setBackupName(backupName);
            backupInfoList.add(backupInfo);
            switch (BusinessEntity.getByName(dataFeedTask.getEntity())) {
                case Account:
                    accountTemplates.add(dataFeedTask.getImportTemplate().getName());
                    break;
                case Contact:
                    contactTemplates.add(dataFeedTask.getImportTemplate().getName());
                    break;
                case Transaction:
                    transactionTemplates.add(dataFeedTask.getImportTemplate().getName());
                    break;
                default:
                    break;
            }
        });
        report.setInputAccountTemplates(accountTemplates);
        report.setInputContactTemplates(contactTemplates);
        report.setInputTransactionTemplates(transactionTemplates);
        report.setBackupTemplateList(backupInfoList);
        migrateTrackingProxy.updateReport(customerSpace.toString(), migrateTrackingPid, report);

        // create or update import system.
        List<S3ImportSystem> currentSystems = cdlProxy.getS3ImportSystemList(customerSpace.toString());
        if (CollectionUtils.isEmpty(currentSystems)) {
            S3ImportSystem importSystem = createDefaultImportSystem(customerSpace, dataFeedTaskMap);
            cdlProxy.createS3ImportSystem(customerSpace.toString(), importSystem);
            dropBoxProxy.createTemplateFolder(customerSpace.toString(), DEFAULT_SYSTEM, null, null);
            report.setSystemName(DEFAULT_SYSTEM);
            putStringValueInContext(PRIMARY_IMPORT_SYSTEM, DEFAULT_SYSTEM);
        } else {
            Optional<S3ImportSystem> s3ImportSystemOptional =
                    currentSystems.stream().filter(importSystem -> importSystem.getPriority() == 1).findFirst();
            if (s3ImportSystemOptional.isPresent()) {
                S3ImportSystem importSystem = s3ImportSystemOptional.get();
                if (dataFeedTaskMap.containsKey(BusinessEntity.Account)) {
                    importSystem.setMapToLatticeAccount(Boolean.TRUE);
                    if (StringUtils.isEmpty(importSystem.getAccountSystemId())) {
                        importSystem.setAccountSystemId(importSystem.generateAccountSystemId());
                    }
                }
                if (dataFeedTaskMap.containsKey(BusinessEntity.Contact)) {
                    importSystem.setMapToLatticeContact(Boolean.TRUE);
                    if (StringUtils.isEmpty(importSystem.getContactSystemId())) {
                        importSystem.setContactSystemId(importSystem.generateContactSystemId());
                    }
                }
                cdlProxy.updateS3ImportSystem(customerSpace.toString(), importSystem);
                report.setSystemName(importSystem.getName());
                putStringValueInContext(PRIMARY_IMPORT_SYSTEM, importSystem.getName());
            } else {
                S3ImportSystem importSystem = createDefaultImportSystem(customerSpace, dataFeedTaskMap);
                cdlProxy.createS3ImportSystem(customerSpace.toString(), importSystem);
                dropBoxProxy.createTemplateFolder(customerSpace.toString(), DEFAULT_SYSTEM, null, null);
                report.setSystemName(DEFAULT_SYSTEM);
                putStringValueInContext(PRIMARY_IMPORT_SYSTEM, DEFAULT_SYSTEM);
            }
        }
        saveOutputValue(WorkflowContextConstants.Outputs.IMPORT_MIGRATE_TRACKING_ID, String.valueOf(migrateTrackingPid));
        migrateTrackingProxy.updateReport(customerSpace.toString(), migrateTrackingPid, report);
    }

    private S3ImportSystem createDefaultImportSystem(CustomerSpace customerSpace,
                                                     Map<BusinessEntity, List<String>> dataFeedTaskMap) {
        S3ImportSystem importSystem = new S3ImportSystem();
        importSystem.setPriority(1);
        importSystem.setName(DEFAULT_SYSTEM);
        importSystem.setDisplayName(DEFAULT_SYSTEM);
        importSystem.setSystemType(S3ImportSystem.SystemType.Other);
        importSystem.setTenant(tenantEntityMgr.findByTenantId(customerSpace.toString()));
        if (dataFeedTaskMap.containsKey(BusinessEntity.Account)) {
            importSystem.setAccountSystemId(importSystem.generateAccountSystemId());
            importSystem.setMapToLatticeAccount(Boolean.TRUE);
        }
        if (dataFeedTaskMap.containsKey(BusinessEntity.Contact)) {
            importSystem.setContactSystemId(importSystem.generateContactSystemId());
            importSystem.setMapToLatticeContact(Boolean.TRUE);
        }
        return importSystem;
    }
}
