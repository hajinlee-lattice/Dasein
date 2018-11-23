package com.latticeengines.serviceflows.workflow.export;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;

@Component("importCleanupFromS3")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ImportCleanupFromS3 extends BaseImportExportS3<ImportExportS3StepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ImportCleanupFromS3.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Override
    protected void buildRequests(List<ImportExportRequest> requests) {
        DataCollection.Version activeVersion = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        if (activeVersion == null) {
            log.info("There's no active version!");
            return;
        }
        List<String> impactEntities = getListObjectFromContext(IMPACTED_ENTITIES, String.class);
        List<TableRoleInCollection> roleList = new ArrayList<>();
        for (String entityName : impactEntities) {
            BusinessEntity entity = BusinessEntity.getByName(entityName);
            switch (entity) {
                case Account:
                case Contact:
                case Product:
                    roleList.add(entity.getBatchStore());
                    break;
                case Transaction:
                    roleList.add(TableRoleInCollection.ConsolidatedRawTransaction);
                    roleList.add(TableRoleInCollection.ConsolidatedDailyTransaction);
                    roleList.add(TableRoleInCollection.ConsolidatedPeriodTransaction);
                    break;
                default:
                    break;
            }
        }
        for (TableRoleInCollection role : roleList) {
            List<String> activeTableNames = dataCollectionProxy.getTableNames(customer, role, activeVersion);
            if (CollectionUtils.isNotEmpty(activeTableNames)) {
                log.info("Start to add active tables for tenant=" + customer + " role=" + role.name());
                activeTableNames.forEach(t -> addTableDir(t, requests));
            }
        }
    }

    private void addTableDir(String tableName, List<ImportExportRequest> requests) {
        if (StringUtils.isBlank(tableName)) {
            return;
        }
        Table table = metadataProxy.getTable(customer, tableName);
        if (table == null) {
            log.warn("Can not find the table=" + tableName + " for tenant=" + customer);
            return;
        }
        addTableToRequestForImport(table, requests);
    }
}
