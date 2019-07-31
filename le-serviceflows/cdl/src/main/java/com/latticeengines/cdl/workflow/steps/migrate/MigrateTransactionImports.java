package com.latticeengines.cdl.workflow.steps.migrate;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.map.HashedMap;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.MigrateTransactionImportStepConfiguration;

@Component(MigrateTransactionImports.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MigrateTransactionImports extends BaseMigrateImports<MigrateTransactionImportStepConfiguration> {

    static final String BEAN_NAME = "migrateTransactionImports";

    private static final String TARGET_TABLE = "MigratedTransactionImport";

    @Override
    protected String getTargetTablePrefix() {
        return TARGET_TABLE;
    }

    @Override
    protected TableRoleInCollection getBatchStore() {
        return TableRoleInCollection.ConsolidatedRawTransaction;
    }

    @Override
    protected Map<String, String> getRenameMap() {
        Map<String, String> renameMap = new HashedMap<>();
        if (templateTable.getAttribute(InterfaceName.CustomerAccountId) != null) {
            renameMap.put(InterfaceName.AccountId.name(), InterfaceName.CustomerAccountId.name());
        }
        if (templateTable.getAttribute(InterfaceName.CustomerContactId) != null) {
            renameMap.put(InterfaceName.ContactId.name(), InterfaceName.CustomerContactId.name());
        }
        return renameMap;
    }

    @Override
    protected Map<String, String> getDuplicateMap() {
        return new HashedMap<>();
    }

    @Override
    protected String getTaskId() {
        if (importMigrateTracking == null) {
            importMigrateTracking = migrateTrackingProxy.getMigrateTracking(customerSpace.toString(),
                    configuration.getMigrateTrackingPid());
        }
        return importMigrateTracking.getReport().getOutputTransactionTaskId();
    }

    @Override
    protected void updateMigrateTracking(Long migratedCounts, List<String> dataTables) {
        importMigrateTracking = migrateTrackingProxy.getMigrateTracking(customerSpace.toString(),
                configuration.getMigrateTrackingPid());
        importMigrateTracking.getReport().setTransactionCounts(migratedCounts);
        importMigrateTracking.getReport().setTransactionDataTables(dataTables);
        migrateTrackingProxy.updateReport(customerSpace.toString(), importMigrateTracking.getPid(), importMigrateTracking.getReport());
    }
}
