package com.latticeengines.serviceflows.workflow.export;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
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

    private static final Logger log = LoggerFactory.getLogger(ImportModelFromS3.class);

    private Version version;

    protected void buildRequests(List<ImportExportRequest> requests) {
        if (CustomEventModelingType.LPI.equals(getConfiguration().getCustomEventModelingType())) {
            return;
        }
        version = getConfiguration().getVersion();
        List<Table> tables = getTables();
        tables.forEach(table -> addTableToRequestForImport(table, requests));
    }

    private List<Table> getTables() {
        List<Table> tables = new ArrayList<>();
        for (TableRoleInCollection role: TableRoleInCollection.values()) {
            addTableIfExists(tables, role);
        }
        return tables;
    }

    private void addTableIfExists(List<Table> accumulator, TableRoleInCollection role) {
        String tableName = dataCollectionProxy.getTableName(customer, role, version);
        if (StringUtils.isNotBlank(tableName)) {
            Table table = metadataProxy.getTableSummary(customer, tableName);
            accumulator.add(table);
        } else {
            log.warn("Did not find a " + role + " table in version " + version);
        }
    }

}
