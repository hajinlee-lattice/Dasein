package com.latticeengines.cdl.workflow.steps.merge;

import static com.latticeengines.domain.exposed.admin.LatticeFeatureFlag.ENABLE_ACCOUNT360;
import static com.latticeengines.domain.exposed.admin.LatticeModule.TalkingPoint;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.AccountLookup;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedAccount;

import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.cdl.workflow.steps.BaseProcessAnalyzeSparkStep;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.DynamoExportConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.GenerateAccountLookupConfig;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.spark.exposed.job.cdl.GenerateAccountLookupJob;

@Component("generateAccountLookup")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class GenerateAccountLookup extends BaseProcessAnalyzeSparkStep<ProcessAccountStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(GenerateAccountLookup.class);
    private static final TableRoleInCollection TABLE_ROLE = AccountLookup;

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Inject
    private BatonService batonService;

    @Value("${cdl.processAnalyze.skip.dynamo.publication}")
    private boolean skipPublishDynamo;

    @Value("${cdl.processAnalyze.accountlookup.legacy.publication.enabled}")
    private boolean legacyPublicationEnabled;

    @Override
    public void execute() {
        bootstrap();
        if (!legacyPublicationEnabled) {
            log.info("Old account lookup to dynamo publication disabled.");
        }
        if (shouldGenerateAccountLookup()) {
            Table tableInCtx = getTableSummaryFromKey(customerSpace.toString(), ACCOUNT_LOOKUP_TABLE_NAME);
            if (tableInCtx == null) {
                GenerateAccountLookupConfig jobConfig = configureJob();
                SparkJobResult result = runSparkJob(GenerateAccountLookupJob.class, jobConfig);
                String tenantId = CustomerSpace.shortenCustomerSpace(customerSpace.toString());
                String resultTableName = tenantId + "_" + NamingUtils.timestamp(TABLE_ROLE.name());
                Table resultTable = toTable(resultTableName, InterfaceName.AtlasLookupKey.name(), result.getTargets().get(0));
                metadataProxy.createTable(customerSpace.toString(), resultTableName, resultTable);
                dataCollectionProxy.upsertTable(customerSpace.toString(), resultTableName, TABLE_ROLE, inactive);
                exportToS3AndAddToContext(resultTable, ACCOUNT_LOOKUP_TABLE_NAME);
                exportToDynamo(resultTableName);
            } else {
                log.info("Found account lookup table in context, skip this step.");
                exportToDynamo(tableInCtx.getName());
            }
        } else {
            log.info("No need to refresh account lookup store.");
            linkInactiveTable(TABLE_ROLE);
        }
    }

    protected GenerateAccountLookupConfig configureJob() {
        TableRoleInCollection batchStore = BusinessEntity.Account.getBatchStore();
        String batchStoreName = dataCollectionProxy.getTableName(customerSpace.toString(), batchStore, inactive);
        Table batchStoreSummary = metadataProxy.getTableSummary(customerSpace.toString(), batchStoreName);
        GenerateAccountLookupConfig config = new GenerateAccountLookupConfig();
        config.setInput(Collections.singletonList(batchStoreSummary.toHdfsDataUnit("Account")));
        List<String> lookupIds = servingStoreProxy
                .getDecoratedMetadata(customerSpace.toString(), BusinessEntity.Account,
                        Collections.singletonList(ColumnSelection.Predefined.LookupId), inactive)
                .map(ColumnMetadata::getAttrName).collectList().block();
        log.info("lookupIds=" + lookupIds);
        config.setLookupIds(lookupIds);
        return config;
    }

    private boolean missingLookupTable() {
        TableRoleInCollection batchStore = BusinessEntity.Account.getBatchStore();
        String activeBatchStoreName = dataCollectionProxy.getTableName(customerSpace.toString(), batchStore, active);
        String activeTableName = dataCollectionProxy.getTableName(customerSpace.toString(), TABLE_ROLE, active);
        return StringUtils.isBlank(activeTableName) && StringUtils.isNotBlank(activeBatchStoreName);
    }

    private boolean shouldGenerateAccountLookup() {
        boolean shouldBuild = false;
        if (isChanged(ConsolidatedAccount)) {
            log.info("Should build AccountLookup, because account batch store has changed.");
            shouldBuild = true;
        } else if (missingLookupTable()) {
            log.info("Should build AccountLookup, because not found in active version.");
            shouldBuild = true;
        }
        return shouldBuild;
    }

    private void exportToDynamo(String tableName) {
        if (shouldPublishDynamo()) {
            String inputPath = metadataProxy.getAvroDir(configuration.getCustomerSpace().toString(), tableName);
            DynamoExportConfig config = new DynamoExportConfig();
            config.setTableName(tableName);
            config.setInputPath(PathUtils.toAvroGlob(inputPath));
            config.setPartitionKey(TABLE_ROLE.getPartitionKey());
            log.info("Queued for DynamoExport with config : " + JsonUtils.serialize(config));
            addToListInContext(TABLES_GOING_TO_DYNAMO, config, DynamoExportConfig.class);
        }
    }

    private boolean shouldPublishDynamo() {
        boolean enableTp = batonService.hasModule(customerSpace, TalkingPoint);
        boolean hasAccount360 = batonService.isEnabled(customerSpace, ENABLE_ACCOUNT360);
        return !skipPublishDynamo && (hasAccount360 || enableTp) && legacyPublicationEnabled;
    }

}
