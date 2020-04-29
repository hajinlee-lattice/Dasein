package com.latticeengines.cdl.workflow.steps.rebuild;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.CloneTableService;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.CuratedContactAttributesStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.DynamoExportConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.GenerateCuratedAttributesConfig;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.cdl.GenerateCuratedAttributes;

@Component(CuratedContactAttributes.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CuratedContactAttributes
        extends RunSparkJob<CuratedContactAttributesStepConfiguration, GenerateCuratedAttributesConfig> {

    private static final Logger log = LoggerFactory.getLogger(CuratedContactAttributes.class);

    public static final String BEAN_NAME = "curatedContactAttributesStep";
    private static final TableRoleInCollection TABLE_ROLE = TableRoleInCollection.CalculatedCuratedContact;
    private static final String CONTACT_LAST_ACTIVITY_TABLE_PREFIX = "LastActivityDate_"
            + BusinessEntity.Contact.name();
    private static final String LAST_ACTIVITY_DATE_DISPLAY_NAME = "Lattice - Last Activity Date";
    private static final String LAST_ACTIVITY_DATE_DESCRIPTION = "Most recent activity date among "
            + "any of the time series activity data (excluding transactions)";

    private DataCollection.Version inactive;
    private DataCollection.Version active;
    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private CloneTableService cloneTableService;

    @Override
    protected CustomerSpace parseCustomerSpace(CuratedContactAttributesStepConfiguration stepConfiguration) {
        if (customerSpace == null) {
            customerSpace = configuration.getCustomerSpace();
        }
        return customerSpace;
    }

    @Override
    protected Class<? extends AbstractSparkJob<GenerateCuratedAttributesConfig>> getJobClz() {
        return GenerateCuratedAttributes.class;
    }

    @Override
    protected GenerateCuratedAttributesConfig configureJob(
            CuratedContactAttributesStepConfiguration stepConfiguration) {
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        Map<String, String> lastActivityTables = getMapObjectFromContext(LAST_ACTIVITY_DATE_TABLE_NAME, String.class,
                String.class);
        String contactLastActivityTempTableName = lastActivityTables.getOrDefault(BusinessEntity.Contact.name(), "");

        if (StringUtils.isBlank(contactLastActivityTempTableName)) {
            log.warn("No temp table for contact activity date found, skipping step");
            cloneTableService.linkInactiveTable(TABLE_ROLE);
            return null;
        }

        GenerateCuratedAttributesConfig jobConfig = new GenerateCuratedAttributesConfig();
        jobConfig.joinKey = InterfaceName.ContactId.name();
        jobConfig.columnsToIncludeFromMaster = Collections.singletonList(InterfaceName.AccountId.name());
        jobConfig.masterTableIdx = 0;
        jobConfig.lastActivityDateInputIdx = 1;
        Table contactLastActivityTempTable = metadataProxy.getTable(customerSpace.getTenantId(),
                contactLastActivityTempTableName);

        if (contactLastActivityTempTable == null) {
            log.warn("No Metadata table found for contact activity date by name:" + contactLastActivityTempTableName
                    + ", skipping step");
            return null;
        }
        Table contactTable = metadataProxy.getTable(customerSpace.getTenantId(), getContactTableName());

        jobConfig.setInput(Arrays.asList(HdfsDataUnit.fromPath(contactTable.getExtracts().get(0).getPath()),
                HdfsDataUnit.fromPath(contactLastActivityTempTable.getExtracts().get(0).getPath())));

        return jobConfig;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        String tenantId = CustomerSpace.shortenCustomerSpace(customerSpace.toString());
        String resultTableName = tenantId + "_" + NamingUtils.timestamp(TABLE_ROLE.name());
        Table resultTable = toTable(resultTableName, InterfaceName.ContactId.name(), result.getTargets().get(0));
        metadataProxy.createTable(customerSpace.toString(), resultTableName, resultTable);
        dataCollectionProxy.upsertTable(customerSpace.toString(), resultTableName, TABLE_ROLE, inactive);
        exportToS3AndAddToContext(resultTable, ACCOUNT_LOOKUP_TABLE_NAME);
        exportToDynamo(resultTableName, TABLE_ROLE.getPartitionKey(), TABLE_ROLE.getRangeKey());
    }

    protected void exportToDynamo(String tableName, String partitionKey, String sortKey) {
        String inputPath = metadataProxy.getAvroDir(configuration.getCustomerSpace().toString(), tableName);
        DynamoExportConfig config = new DynamoExportConfig();
        config.setTableName(tableName);
        config.setInputPath(PathUtils.toAvroGlob(inputPath));
        config.setPartitionKey(partitionKey);
        if (StringUtils.isNotBlank(sortKey)) {
            config.setSortKey(sortKey);
        }
        addToListInContext(TABLES_GOING_TO_DYNAMO, config, DynamoExportConfig.class);
    }

    private String getContactTableName() {
        return getTableName(BusinessEntity.Contact.getBatchStore(), "contact batch store");
    }

    private String getTableName(@NotNull TableRoleInCollection role, @NotNull String name) {
        String tableName = dataCollectionProxy.getTableName(customerSpace.toString(), role, inactive);
        if (StringUtils.isBlank(tableName)) {
            tableName = dataCollectionProxy.getTableName(customerSpace.toString(), role, active);
            if (StringUtils.isNotBlank(tableName)) {
                log.info("Found {} (role={}) in active version {}", name, role, active);
            }
        } else {
            log.info("Found {} (role={}) in inactive version {}", name, role, inactive);
        }
        return tableName;
    }
}
