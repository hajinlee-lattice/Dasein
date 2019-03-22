package com.latticeengines.serviceflows.workflow.export;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataCollection.Version;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.modeling.CustomEventModelingType;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;
import com.latticeengines.serviceflows.workflow.util.ImportExportRequest;

@Component("importModelFromS3")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ImportModelFromS3 extends BaseImportExportS3<ImportExportS3StepConfiguration> {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(ImportModelFromS3.class);

    protected void buildRequests(List<ImportExportRequest> requests) {
        if (CustomEventModelingType.LPI.equals(getConfiguration().getCustomEventModelingType())) {
            return;
        }
        List<Table> tables = new ArrayList<>();
        getTables(tables);
        tables.forEach(table -> {
            addTableToRequestForImport(table, requests);
        });

    }

    private void getTables(List<Table> tables) {
        Version version = getConfiguration().getVersion();
        Table accountTable = dataCollectionProxy.getTable(getConfiguration().getCustomerSpace().toString(),
                TableRoleInCollection.ConsolidatedAccount, version);
        if (accountTable != null) {
            tables.add(accountTable);
        }
        Table featureTable = dataCollectionProxy.getTable(customer, TableRoleInCollection.AccountFeatures, version);
        if (featureTable != null) {
            tables.add(featureTable);
        }
        Table apsTable = dataCollectionProxy.getTable(customer, TableRoleInCollection.AnalyticPurchaseState, version);
        if (apsTable != null) {
            tables.add(apsTable);
        }
    }

}
