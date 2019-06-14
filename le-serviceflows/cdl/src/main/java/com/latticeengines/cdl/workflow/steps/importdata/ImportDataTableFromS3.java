package com.latticeengines.cdl.workflow.steps.importdata;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.serviceflows.workflow.export.BaseImportExportS3;
import com.latticeengines.serviceflows.workflow.util.ImportExportRequest;

@Component("importDataFeedFromS3")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ImportDataTableFromS3 extends BaseImportExportS3<ImportExportS3StepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ImportDataTableFromS3.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Override
    protected void buildRequests(List<ImportExportRequest> requests) {
        buildImportedRequests(requests);
    }

    @SuppressWarnings("rawtypes")
    private void buildImportedRequests(List<ImportExportRequest> requests) {
        CustomerSpace customerSpace = configuration.getCustomerSpace();
        Table productTable = getCurrentConsolidateProductTable(customerSpace);
        if (productTable == null) {
            return;
        }
        addTableToRequestForImport(productTable, requests);

    }

    private Table getCurrentConsolidateProductTable(CustomerSpace customerSpace) {
        DataCollection.Version activeVersion = dataCollectionProxy.getActiveVersion(customerSpace.toString());
        DataCollection.Version inactiveVersion = activeVersion.complement();
        Table currentTable = dataCollectionProxy.getTable(customerSpace.toString(),
                TableRoleInCollection.ConsolidatedProduct, activeVersion);
        if (currentTable != null) {
            log.info("Found consolidated product table with version " + activeVersion);
            return currentTable;
        }

        currentTable = dataCollectionProxy.getTable(customerSpace.toString(), TableRoleInCollection.ConsolidatedProduct,
                inactiveVersion);
        if (currentTable != null) {
            log.info("Found consolidated product table with version " + inactiveVersion);
            return currentTable;
        }

        log.info("There is no ConsolidatedProduct table with version " + activeVersion + " and " + inactiveVersion);
        return null;
    }

}
