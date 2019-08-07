package com.latticeengines.cdl.workflow.steps.migrate;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.map.HashedMap;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.MigrateAccountImportStepConfiguration;

@Component(MigrateAccountImports.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MigrateAccountImports extends BaseMigrateImports<MigrateAccountImportStepConfiguration> {

    static final String BEAN_NAME = "migrateAccountImports";

    private static final String TARGET_TABLE = "MigratedAccountImport";

    @Override
    protected String getTargetTablePrefix() {
        return TARGET_TABLE;
    }

    @Override
    protected TableRoleInCollection getBatchStore() {
        return BusinessEntity.Account.getBatchStore();
    }

    @Override
    protected Map<String, String> getRenameMap() {
        Map<String, String> renameMap = new HashedMap<>();
        if (templateTable.getAttribute(InterfaceName.CustomerAccountId) != null) {
            renameMap.put(InterfaceName.AccountId.name(), InterfaceName.CustomerAccountId.name());
        }
        return renameMap;
    }

    @Override
    protected Map<String, String> getDuplicateMap() {
        Map<String, String> dupMap = new HashedMap<>();
        if (StringUtils.isNotEmpty(importSystem.getAccountSystemId())) {
            dupMap.put(InterfaceName.AccountId.name(), importSystem.getAccountSystemId());
        }
        return dupMap;
    }

    @Override
    protected String getTaskId() {
        if (importMigrateTracking == null) {
            importMigrateTracking = migrateTrackingProxy.getMigrateTracking(customerSpace.toString(),
                    configuration.getMigrateTrackingPid());
        }
        return importMigrateTracking.getReport().getOutputAccountTaskId();
    }

    @Override
    protected void updateMigrateTracking(Long migratedCounts, List<String> dataTables) {
        importMigrateTracking = migrateTrackingProxy.getMigrateTracking(customerSpace.toString(),
                configuration.getMigrateTrackingPid());
        importMigrateTracking.getReport().setAccountCounts(migratedCounts);
        importMigrateTracking.getReport().setAccountDataTables(dataTables);
        migrateTrackingProxy.updateReport(customerSpace.toString(), importMigrateTracking.getPid(), importMigrateTracking.getReport());
    }

    @Override
    protected PipelineTransformationRequest generateRequest() {
        return null;
    }
}
