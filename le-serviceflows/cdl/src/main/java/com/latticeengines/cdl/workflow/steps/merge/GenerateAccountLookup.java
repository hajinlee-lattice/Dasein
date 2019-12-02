package com.latticeengines.cdl.workflow.steps.merge;

import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.DynamoExportConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.GenerateAccountLookupConfig;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.cdl.GenerateAccountLookupJob;

@Component("generateAccountLookup")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class GenerateAccountLookup extends RunSparkJob<ProcessAccountStepConfiguration, GenerateAccountLookupConfig> {

    private static final Logger log = LoggerFactory.getLogger(GenerateAccountLookup.class);
    private static final TableRoleInCollection TABLE_ROLE = TableRoleInCollection.AccountLookup;

    private CustomerSpace customerSpace;
    private DataCollection.Version active;
    private DataCollection.Version inactive;

    private String activeBatchStoreName;
    private String inactiveBatchStoreName;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Override
    protected Class<? extends AbstractSparkJob<GenerateAccountLookupConfig>> getJobClz() {
        return GenerateAccountLookupJob.class;
    }

    @Override
    protected CustomerSpace parseCustomerSpace(ProcessAccountStepConfiguration stepConfiguration) {
        return stepConfiguration.getCustomerSpace();
    }

    @Override
    protected GenerateAccountLookupConfig configureJob(ProcessAccountStepConfiguration stepConfiguration) {
        customerSpace = stepConfiguration.getCustomerSpace();
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        if (hasNewBatchStore() || missingLookupTable()) {
            String batchStoreName = hasNewBatchStore() ? inactiveBatchStoreName : activeBatchStoreName;
            Table batchStoreSummary = metadataProxy.getTableSummary(customerSpace.toString(), batchStoreName);
            GenerateAccountLookupConfig config = new GenerateAccountLookupConfig();
            config.setInput(Collections.singletonList(batchStoreSummary.toHdfsDataUnit("Account")));
            List<String> lookupIds = servingStoreProxy
                    .getDecoratedMetadata(customerSpace.toString(), BusinessEntity.Account,
                            Collections.singletonList(ColumnSelection.Predefined.LookupId))
                    .map(ColumnMetadata::getAttrName).collectList().block();
            log.info("lookupIds=" + lookupIds);
            config.setLookupIds(lookupIds);
            return config;
        } else {
            log.info("There is no reason to build AccountLookup table.");
            return null;
        }
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        String tenantId = CustomerSpace.shortenCustomerSpace(customerSpace.toString());
        String resultTableName = tenantId + "_" + NamingUtils.timestamp(TABLE_ROLE.name());
        Table resultTable = toTable(resultTableName, InterfaceName.AtlasLookupKey.name(), result.getTargets().get(0));
        metadataProxy.createTable(customerSpace.toString(), resultTableName, resultTable);
        dataCollectionProxy.upsertTable(customerSpace.toString(), resultTableName, TABLE_ROLE, inactive);
        exportToDynamo(resultTableName);
    }

    private boolean missingLookupTable() {
        if (StringUtils.isBlank(activeBatchStoreName)) {
            TableRoleInCollection batchStore = BusinessEntity.Account.getBatchStore();
            activeBatchStoreName = dataCollectionProxy.getTableName(customerSpace.toString(), batchStore, active);
        }
        String activeTableName = dataCollectionProxy.getTableName(customerSpace.toString(), TABLE_ROLE, active);
        return StringUtils.isBlank(activeTableName) && StringUtils.isNotBlank(activeBatchStoreName);
    }

    private boolean hasNewBatchStore() {
        TableRoleInCollection batchStore = BusinessEntity.Account.getBatchStore();
        if (StringUtils.isBlank(activeBatchStoreName)) {
            activeBatchStoreName = dataCollectionProxy.getTableName(customerSpace.toString(), batchStore, active);
        }
        if (StringUtils.isBlank(inactiveBatchStoreName)) {
            inactiveBatchStoreName = dataCollectionProxy.getTableName(customerSpace.toString(), batchStore, inactive);
        }
        return StringUtils.isNotBlank(inactiveBatchStoreName) && !inactiveBatchStoreName.equals(activeBatchStoreName);
    }

    private void exportToDynamo(String tableName) {
        String inputPath = metadataProxy.getAvroDir(configuration.getCustomerSpace().toString(), tableName);
        DynamoExportConfig config = new DynamoExportConfig();
        config.setTableName(tableName);
        config.setInputPath(PathUtils.toAvroGlob(inputPath));
        config.setPartitionKey(TABLE_ROLE.getPrimaryKey().name());
        addToListInContext(TABLES_GOING_TO_DYNAMO, config, DynamoExportConfig.class);
    }

}
