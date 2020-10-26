package com.latticeengines.serviceflows.workflow.export;

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
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportTableRoleFromS3StepConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.serviceflows.workflow.util.ImportExportRequest;

@Component("importTableRoleFromS3")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ImportTableRoleFromS3 extends BaseImportExportS3<ImportTableRoleFromS3StepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ImportTableRoleFromS3.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Override
    protected void buildRequests(List<ImportExportRequest> requests) {
        buildBusinessEntityRequests(requests);
    }

    private void buildBusinessEntityRequests(List<ImportExportRequest> requests) {
        DataCollection.Version activeVersion = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        if (activeVersion == null) {
            activeVersion = dataCollectionProxy.getActiveVersion(customer);
        }
        for (TableRoleInCollection role : configuration.getTableRoleInCollections()) {
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
