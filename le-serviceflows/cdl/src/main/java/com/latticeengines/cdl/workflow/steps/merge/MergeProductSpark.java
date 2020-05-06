package com.latticeengines.cdl.workflow.steps.merge;

import static com.latticeengines.cdl.workflow.steps.merge.MergeProductSpark.BEAN_NAME;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.cdl.DataLimit;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.cdl.ReportConstants;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessProductStepConfiguration;
import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.MergeProductConfig;
import com.latticeengines.domain.exposed.spark.cdl.MergeProductReport;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.proxy.exposed.cdl.CDLAttrConfigProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.BaseSparkStep;

@Component(BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MergeProductSpark extends BaseSparkStep<ProcessProductStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(MergeProductSpark.class);
    static final String BEAN_NAME = "mergeProductSpark";

    @Inject
    protected DataCollectionProxy dataCollectionProxy;

    @Inject
    protected MetadataProxy metadataProxy;

    @Inject
    private MatchProxy matchProxy;

    @Inject
    protected CDLAttrConfigProxy cdlAttrConfigProxy;

    private DataCollection.Version active;
    private DataCollection.Version inactive;

    @Override
    public void execute() {
        customerSpace = configuration.getCustomerSpace();
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        String tenantId = customerSpace.getTenantId();
        String jobName = tenantId + "~" + MergeProductSpark.class.getSimpleName();
        MergeProductConfig jobConfig = new MergeProductConfig();
        jobConfig.setInput(getInputs());
        jobConfig.setWorkspace(getRandomWorkspace());
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        SparkJobResult result = retry.execute(ctx -> {
            if (ctx.getRetryCount() > 0) {
                log.info("(Attempt=" + (ctx.getRetryCount() + 1) + ") run merge product spark job.");
                log.warn("Previous failure:",  ctx.getLastThrowable());
            }
            try {
                LivySession session = createLivySession(jobName);
                return runSparkJob(session, com.latticeengines.spark.exposed.job.cdl.MergeProduct.class, jobConfig);
            } finally {
                killLivySession();
            }
        });

        TableRoleInCollection role = TableRoleInCollection.ConsolidatedProduct;
        String tableName = NamingUtils.timestamp(role.name());
        Table productTable = toTable(tableName, result.getTargets().get(0));
        metadataProxy.createTable(customerSpace.toString(), tableName, productTable);
        dataCollectionProxy.upsertTable(customerSpace.toString(), tableName, role, active);

        MergeProductReport report = JsonUtils.deserialize(result.getOutput(), MergeProductReport.class);
        DataLimit dataLimit = getObjectFromContext(DATAQUOTA_LIMIT, DataLimit.class);
        if (dataLimit.getProductBundleDataQuotaLimit() < report.getAnalyticProducts()) {
            throw new IllegalStateException(
                    "the Analytics Product data quota limit is " + dataLimit.getProductBundleDataQuotaLimit()
                            + ", The data you uploaded has exceeded the limit.");
        }
        if (dataLimit.getProductSkuDataQuotaLimit() < report.getSpendingProducts()) {
            throw new IllegalStateException(
                    "the Spending Product data quota limit is " + dataLimit.getProductSkuDataQuotaLimit()
                            + ", The data you uploaded has exceeded the limit.");
        }
        updateReport(report);
    }

    private List<DataUnit> getInputs() {
        List<DataUnit> inputs = new ArrayList<>();
        HdfsDataUnit newProducts = getNewProducts();
        inputs.add(newProducts);
        HdfsDataUnit oldProducts = getOldProducts();
        if (oldProducts != null) {
            inputs.add(oldProducts);
        }
        return inputs;
    }

    private HdfsDataUnit getNewProducts() {
        String tableName = getStringValueFromContext(MERGED_PRODUCT_IMPORTS);
        return metadataProxy.getTable(customerSpace.toString(), tableName).toHdfsDataUnit("new");
    }

    private HdfsDataUnit getOldProducts() {
        Table currentTable = dataCollectionProxy.getTable(customerSpace.toString(),
                TableRoleInCollection.ConsolidatedProduct, active);
        if (currentTable != null) {
            log.info("Found consolidated product table with version " + active);
        } else {
            currentTable = dataCollectionProxy.getTable(customerSpace.toString(), TableRoleInCollection.ConsolidatedProduct,
                    inactive);
            if (currentTable != null) {
                log.info("Found consolidated product table with version " + inactive);
            }
        }
        if (currentTable == null) {
            log.info("There is no ConsolidatedProduct table with version " + active + " and " + inactive);
            return null;
        } else {
            return currentTable.toHdfsDataUnit("old");
        }
    }

    private void updateReport(MergeProductReport mergeReport) {
        BusinessEntity entity = BusinessEntity.Product;
        // update product report
        ObjectNode report = getObjectFromContext(ReportPurpose.PROCESS_ANALYZE_RECORDS_SUMMARY.getKey(),
                ObjectNode.class);
        JsonNode entitiesSummaryNode = report.get(ReportPurpose.ENTITIES_SUMMARY.getKey());
        if (entitiesSummaryNode == null) {
            entitiesSummaryNode = report.putObject(ReportPurpose.ENTITIES_SUMMARY.getKey());
        }
        JsonNode entityNode = entitiesSummaryNode.get(entity.name());
        if (entityNode == null) {
            entityNode = ((ObjectNode) entitiesSummaryNode).putObject(entity.name());
        }
        JsonNode consolidateSummaryNode = entityNode.get(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey());
        if (consolidateSummaryNode == null) {
            consolidateSummaryNode = ((ObjectNode) entityNode)
                    .putObject(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey());
        }

        ((ObjectNode) consolidateSummaryNode).put(ReportConstants.PRODUCT_ID, mergeReport.getRecords());
        ((ObjectNode) consolidateSummaryNode).put(ReportConstants.PRODUCT_HIERARCHY, mergeReport.getHierarchyProducts());
        ((ObjectNode) consolidateSummaryNode).put(ReportConstants.PRODUCT_BUNDLE, mergeReport.getBundleProducts());

        List<String> errors = mergeReport.getErrors();
        if (CollectionUtils.isNotEmpty(errors)) {
            errors.forEach(log::warn);
            ((ObjectNode) consolidateSummaryNode).put(ReportConstants.WARN_MESSAGE, errors.get(0));
        }
        putObjectInContext(ReportPurpose.PROCESS_ANALYZE_RECORDS_SUMMARY.getKey(), report);


        Map<BusinessEntity, Integer> finalRecordsMap = //
                getMapObjectFromContext(FINAL_RECORDS, BusinessEntity.class, Integer.class);
        if (finalRecordsMap == null) {
            finalRecordsMap = new HashMap<>();
        }
        finalRecordsMap.put(BusinessEntity.Product, Long.valueOf(mergeReport.getAnalyticProducts()).intValue());
        finalRecordsMap.put(BusinessEntity.ProductHierarchy, Long.valueOf(mergeReport.getHierarchyProducts()).intValue());
        putObjectInContext(FINAL_RECORDS, finalRecordsMap);
        log.info("FINAL_RECORDS={}", JsonUtils.serialize(finalRecordsMap));
    }

}
