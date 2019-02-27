package com.latticeengines.serviceflows.workflow.export;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;
import com.latticeengines.common.exposed.util.JsonUtils;

@Component("importTablesForOrphanReportFromS3")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ImportTablesForOrphanReportFromS3 extends BaseImportExportS3<ImportExportS3StepConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(ImportTablesForOrphanReportFromS3.class);

    @Override
    protected void buildRequests(List<ImportExportRequest> importExportRequests) {
        List<Table> tables = getTables();
        log.info("Got tables=" + JsonUtils.serialize(tables));
        tables.forEach(table -> addTableToRequestForImport(table, importExportRequests));
    }

    private List<Table> getTables() {
        List<Table> tables = new ArrayList<>();
        DataCollection.Version version = configuration.getVersion();
        Table transactionTable = dataCollectionProxy.getTable(customer,
                TableRoleInCollection.ConsolidatedRawTransaction, version);
        Table accountTable = dataCollectionProxy.getTable(customer,
                TableRoleInCollection.ConsolidatedAccount, version);
        Table contactTable = dataCollectionProxy.getTable(customer,
                TableRoleInCollection.ConsolidatedContact, version);
        Table productTable = dataCollectionProxy.getTable(customer,
                TableRoleInCollection.ConsolidatedProduct, version);
        if (transactionTable != null) {
            tables.add(transactionTable);
        }
        if (accountTable != null) {
            tables.add(accountTable);
        }
        if (contactTable != null) {
            tables.add(contactTable);
        }
        if (productTable != null) {
            tables.add(productTable);
        }
        return tables;
    }
}
