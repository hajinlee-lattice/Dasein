package com.latticeengines.cdl.workflow.steps.migrate;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.map.HashedMap;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.MigrateContactImportStepConfiguration;

@Component(MigrateContactImports.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MigrateContactImports extends BaseMigrateImports<MigrateContactImportStepConfiguration> {

    static final String BEAN_NAME = "migrateContactImports";

    private static final String TARGET_TABLE = "MigratedContactImport";

    @Override
    protected String getTargetTablePrefix() {
        return TARGET_TABLE;
    }

    @Override
    protected TableRoleInCollection getBatchStore() {
        return BusinessEntity.Contact.getBatchStore();
    }

    @Override
    protected Map<String, String> getRenameMap() {
        Map<String, String> renameMap = new HashedMap<>();
        if (templateTable.getAttribute(InterfaceName.CustomerContactId) != null) {
            renameMap.put(InterfaceName.ContactId.name(), InterfaceName.CustomerContactId.name());
        }
        return renameMap;
    }

    @Override
    protected Map<String, String> getDuplicateMap() {
        Map<String, String> dupMap = new HashedMap<>();
        if (StringUtils.isNotEmpty(importSystem.getContactSystemId())) {
            dupMap.put(InterfaceName.ContactId.name(), importSystem.getContactSystemId());
        }
        return dupMap;
    }

    @Override
    protected String getTaskId() {
        if (importMigrateTracking == null) {
            importMigrateTracking = migrateTrackingProxy.getMigrateTracking(customerSpace.toString(),
                    configuration.getMigrateTrackingPid());
        }
        return importMigrateTracking.getReport().getOutputContactTaskId();
    }

    @Override
    protected void updateMigrateTracking(Long migratedCounts, List<String> dataTables) {
        importMigrateTracking = migrateTrackingProxy.getMigrateTracking(customerSpace.toString(),
                configuration.getMigrateTrackingPid());
        importMigrateTracking.getReport().setContactCounts(migratedCounts);
        importMigrateTracking.getReport().setContactDataTables(dataTables);
        migrateTrackingProxy.updateReport(customerSpace.toString(), importMigrateTracking.getPid(), importMigrateTracking.getReport());
    }
}
