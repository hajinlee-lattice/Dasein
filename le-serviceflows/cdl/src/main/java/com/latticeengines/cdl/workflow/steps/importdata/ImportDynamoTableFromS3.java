package com.latticeengines.cdl.workflow.steps.importdata;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.ImportDynamoTableFromS3Configuration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.serviceflows.workflow.export.BaseImportExportS3;
import com.latticeengines.serviceflows.workflow.util.ImportExportRequest;

@Component("importDynamoTableFromS3")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ImportDynamoTableFromS3 extends BaseImportExportS3<ImportDynamoTableFromS3Configuration> {

    private static final Logger log = LoggerFactory.getLogger(ImportDynamoTableFromS3.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Override
    protected void buildRequests(List<ImportExportRequest> requests) {
        buildImportedRequests(requests);
    }

    @SuppressWarnings("rawtypes")
    private void buildImportedRequests(List<ImportExportRequest> requests) {
        List<String> tableNames = configuration.getTableNames();
        if (CollectionUtils.isNotEmpty(tableNames)) {
            CustomerSpace customerSpace = configuration.getCustomerSpace();
            tableNames.forEach(tblName -> {
                Table table = metadataProxy.getTable(customerSpace.toString(), tblName);
                if (table != null) {
                    addTableToRequestForImport(table, requests);
                }
            });
        }
    }
}
