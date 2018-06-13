package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.CEAttr;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_BUCKETER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_PROFILER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SORTER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_STATS_CALCULATOR;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ActivityMetricsCuratorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ActivityMetricsPivotConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.CalculateStatsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ProfileConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.SorterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.transaction.ActivityType;
import com.latticeengines.domain.exposed.metadata.transaction.Product;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;
import com.latticeengines.domain.exposed.serviceapps.cdl.ReportConstants;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;
import com.latticeengines.domain.exposed.util.ActivityMetricsUtils;
import com.latticeengines.domain.exposed.util.PeriodStrategyUtils;
import com.latticeengines.domain.exposed.util.ProductUtils;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.proxy.exposed.cdl.ActivityMetricsProxy;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;

@Component(ProfilePurchaseHistory.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProfilePurchaseHistory extends BaseSingleEntityProfileStep<ProcessTransactionStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ProfilePurchaseHistory.class);

    public static final String BEAN_NAME = "profilePurchaseHistory";

    private int curateStep, pivotStep, profileStep, bucketStep;
    private Map<String, List<Product>> productMap;
    private String dailyTableName;
    private String accountTableName;
    private String productTableName;
    private List<String> periodTableNames;
    private List<PeriodStrategy> periodStrategies;
    private List<ActivityMetrics> purchaseMetrics;
    private String evaluationDate;
    private boolean accountHasSegment = false;

    private String curatedMetricsTablePrefix;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private PeriodProxy periodProxy;

    @Inject
    private ActivityMetricsProxy metricsProxy;

    @Inject
    private ActionProxy actionProxy;

    @Override
    protected BusinessEntity getEntity() {
        return BusinessEntity.PurchaseHistory;
    }

    @Override
    protected TableRoleInCollection profileTableRole() {
        return TableRoleInCollection.PurchaseHistoryProfile;
    }

    @Override
    protected PipelineTransformationRequest getTransformRequest() {
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("CalculatePurchaseHistory");
        request.setSubmitter(customerSpace.getTenantId());
        request.setKeepTemp(false);
        request.setEnableSlack(false);
        // -----------
        List<TransformationStepConfig> steps = new ArrayList<>();

        curateStep = 0;
        pivotStep = 1;
        profileStep = 2;
        bucketStep = 3;

        TransformationStepConfig curate = curate();
        TransformationStepConfig pivot = pivot();
        TransformationStepConfig profile = profile();
        TransformationStepConfig bucket = bucket();
        TransformationStepConfig calc = calcStats();
        TransformationStepConfig sortProfile = sortProfile();
        steps.add(curate);
        steps.add(pivot);
        steps.add(profile);
        steps.add(bucket);
        steps.add(calc);
        steps.add(sortProfile);

        // -----------
        request.setSteps(steps);
        return request;
    }

    @Override
    protected void onPostTransformationCompleted() {
        String curatedMetricsTableName = TableUtils.getFullTableName(curatedMetricsTablePrefix, pipelineVersion);
        if (metadataProxy.getTable(customerSpace.toString(), curatedMetricsTableName) == null) {
            throw new IllegalStateException("Cannot find result curated metrics table");
        }

        Table curatedMetricsTable = metadataProxy.getTable(customerSpace.toString(), curatedMetricsTableName);
        curatedMetricsTableName = renameServingStoreTable(BusinessEntity.DepivotedPurchaseHistory, curatedMetricsTable);
        exportTableRoleToRedshift(curatedMetricsTableName, BusinessEntity.DepivotedPurchaseHistory.getServingStore());
        dataCollectionProxy.upsertTable(customerSpace.toString(), curatedMetricsTableName,
                BusinessEntity.DepivotedPurchaseHistory.getServingStore(), inactive);

        super.onPostTransformationCompleted();
        generateReport();
    }

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();
        loadProductMap();

        dailyTableName = getDailyTableName();
        if (StringUtils.isBlank(dailyTableName)) {
            throw new IllegalStateException("Cannot find daily table.");
        }

        accountTableName = getAccountTableName();
        if (StringUtils.isBlank(accountTableName)) {
            throw new IllegalStateException("Cannot find account master table.");
        }

        accountHasSegment = isAccountHasSegment();

        evaluationDate = findEvaluationDate();
        if (StringUtils.isBlank(evaluationDate)) {
            DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE;
            evaluationDate = LocalDate.now().format(formatter);
        }

        periodStrategies = periodProxy.getPeriodStrategies(customerSpace.toString());
        if (CollectionUtils.isEmpty(periodStrategies)) {
            throw new IllegalStateException("Cannot find period strategies");
        }

        purchaseMetrics = metricsProxy.getActivityMetrics(customerSpace.toString(), ActivityType.PurchaseHistory);
        if (purchaseMetrics == null) {
            purchaseMetrics = new ArrayList<>();
        }
        // HasPurchased is the default metrics to calculate
        purchaseMetrics.add(createHasPurchasedMetrics());

        periodTableNames = getPeriodTableNames();
        if (CollectionUtils.isEmpty(periodTableNames)) {
            throw new IllegalStateException("Cannot find period stores");
        }
        periodTableNames = selectPeriodTables(periodTableNames, purchaseMetrics);

        curatedMetricsTablePrefix = TableRoleInCollection.CalculatedDepivotedPurchaseHistory.name();
    }

    private String getDailyTableName() {
        String dailyTableName = dataCollectionProxy.getTableName(customerSpace.toString(),
                TableRoleInCollection.ConsolidatedDailyTransaction, inactive);
        if (StringUtils.isBlank(dailyTableName)) {
            dailyTableName = dataCollectionProxy.getTableName(customerSpace.toString(),
                    TableRoleInCollection.ConsolidatedDailyTransaction, active);
            if (StringUtils.isNotBlank(dailyTableName)) {
                log.info("Found daily table in active version " + active);
            }
        } else {
            log.info("Found daily table in inactive version " + inactive);
        }
        return dailyTableName;
    }

    private String getAccountTableName() {
        String accountTableName = dataCollectionProxy.getTableName(customerSpace.toString(),
                TableRoleInCollection.ConsolidatedAccount, inactive);
        if (StringUtils.isBlank(accountTableName)) {
            accountTableName = dataCollectionProxy.getTableName(customerSpace.toString(),
                    TableRoleInCollection.ConsolidatedAccount, active);
            if (StringUtils.isNotBlank(accountTableName)) {
                log.info("Found account batch store in active version " + active);
            }
        } else {
            log.info("Found account batch store in inactive version " + inactive);
        }
        return accountTableName;
    }

    private boolean isAccountHasSegment() {
        if (StringUtils.isBlank(accountTableName)) {
            accountTableName = getAccountTableName();
        }
        List<ColumnMetadata> cms = metadataProxy.getTableColumns(customerSpace.toString(), accountTableName);
        for (ColumnMetadata cm : cms) {
            if (cm.getAttrName().equals(InterfaceName.SpendAnalyticsSegment.name())) {
                log.info(
                        "Account table has SpendAnalyticsSegment field which is needed in ShareOfWallet calculation");
                return true;
            }
        }
        log.info(
                "Account table does not have SpendAnalyticsSegment field which is needed in ShareOfWallet calculation");
        return false;
    }

    private List<String> getPeriodTableNames() {
        List<String> periodTables = dataCollectionProxy.getTableNames(customerSpace.toString(),
                TableRoleInCollection.ConsolidatedPeriodTransaction, inactive);
        if (CollectionUtils.isEmpty(periodTables)) {
            periodTables = dataCollectionProxy.getTableNames(customerSpace.toString(),
                    TableRoleInCollection.ConsolidatedPeriodTransaction, active);
            if (CollectionUtils.isNotEmpty(periodTables)) {
                log.info("Found period stores in active version " + active);
            }
        } else {
            log.info("Found period stores in inactive version " + inactive);
        }
        return periodTables;
    }

    private List<String> selectPeriodTables(List<String> periodTables, List<ActivityMetrics> metrics) {
        Set<String> periods = new HashSet<>();
        metrics.forEach(m -> {
            periods.add(m.getPeriodsConfig().get(0).getPeriod());
        });
        return PeriodStrategyUtils.filterPeriodTablesByPeriods(periodTables, periods);
    }

    private String findEvaluationDate() {
        String evaluationDate = getStringValueFromContext(CDL_EVALUATION_DATE);
        if (StringUtils.isBlank(evaluationDate)) {
            log.error("Fail to find evaluation date from workflow context");
            evaluationDate = periodProxy.getEvaluationDate(customerSpace.toString());
        }
        log.info("Evaluation date for purchase history profiling: " + evaluationDate);
        return evaluationDate;
    }

    private void loadProductMap() {
        Table productTable = dataCollectionProxy.getTable(customerSpace.toString(),
                TableRoleInCollection.ConsolidatedProduct, inactive);
        if (productTable == null) {
            log.info("Did not find product table in inactive version.");
            productTable = dataCollectionProxy.getTable(customerSpace.toString(),
                    TableRoleInCollection.ConsolidatedProduct, active);
            if (productTable == null) {
                throw new IllegalStateException("Cannot find the product table in both versions");
            }
        }
        log.info(String.format("productTableName for customer %s is %s", configuration.getCustomerSpace().toString(),
                productTable.getName()));
        List<Product> productList = new ArrayList<>(
                ProductUtils.loadProducts(yarnConfiguration, productTable.getExtracts().get(0).getPath()));
        productMap = ProductUtils.getProductMap(productList, ProductType.Analytic.name());
        productTableName = productTable.getName();
    }

    private TransformationStepConfig curate() {
        TransformationStepConfig step = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<>(periodTableNames);
        baseSources.add(accountTableName);
        baseSources.add(productTableName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        SourceTable accountSourceTable = new SourceTable(accountTableName, customerSpace);
        SourceTable productSourceTable = new SourceTable(productTableName, customerSpace);
        baseTables.put(productTableName, productSourceTable);
        baseTables.put(accountTableName, accountSourceTable);
        for (String periodTableName : periodTableNames) {
            SourceTable periodSourceTable = new SourceTable(periodTableName, customerSpace);
            baseTables.put(periodTableName, periodSourceTable);
        }
        step.setBaseTables(baseTables);
        step.setTransformer(DataCloudConstants.ACTIVITY_METRICS_CURATOR);
        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(curatedMetricsTablePrefix);
        targetTable.setPrimaryKey(InterfaceName.__Composite_Key__.name());
        step.setTargetTable(targetTable);

        ActivityMetricsCuratorConfig conf = new ActivityMetricsCuratorConfig();
        conf.setGroupByFields(Arrays.asList(InterfaceName.AccountId.name(), InterfaceName.ProductId.name()));
        conf.setCurrentDate(evaluationDate);
        conf.setMetrics(purchaseMetrics);
        conf.setPeriodStrategies(periodStrategies);
        conf.setType(ActivityType.PurchaseHistory);
        conf.setReduced(true);
        conf.setAccountHasSegment(accountHasSegment);
        conf.setPeriodTableCnt(periodTableNames.size());

        step.setConfiguration(JsonUtils.serialize(conf));
        return step;
    }

    private TransformationStepConfig pivot() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(curateStep));
        List<String> baseSources = Arrays.asList(accountTableName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        SourceTable accountSourceTable = new SourceTable(accountTableName, customerSpace);
        baseTables.put(accountTableName, accountSourceTable);
        step.setBaseTables(baseTables);
        step.setTransformer(DataCloudConstants.ACTIVITY_METRICS_PIVOT);
        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(servingStoreTablePrefix);
        step.setTargetTable(targetTable);

        ActivityMetricsPivotConfig conf = new ActivityMetricsPivotConfig();
        conf.setActivityType(ActivityType.PurchaseHistory);
        conf.setGroupByField(InterfaceName.AccountId.name());
        conf.setPivotField(InterfaceName.ProductId.name());
        conf.setProductMap(productMap);
        conf.setExpanded(true);
        conf.setMetrics(purchaseMetrics);

        step.setConfiguration(JsonUtils.serialize(conf));
        return step;
    }

    private TransformationStepConfig profile() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(pivotStep));
        step.setTransformer(TRANSFORMER_PROFILER);
        ProfileConfig conf = new ProfileConfig();
        conf.setEncAttrPrefix(CEAttr);
        String confStr = appendEngineConf(conf, lightEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    private TransformationStepConfig bucket() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Arrays.asList(pivotStep, profileStep));
        step.setTransformer(TRANSFORMER_BUCKETER);
        step.setConfiguration(emptyStepConfig(lightEngineConfig()));
        return step;
    }

    private TransformationStepConfig calcStats() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Arrays.asList(bucketStep, profileStep));
        step.setTransformer(TRANSFORMER_STATS_CALCULATOR);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(statsTablePrefix);
        step.setTargetTable(targetTable);

        CalculateStatsConfig conf = new CalculateStatsConfig();
        step.setConfiguration(appendEngineConf(conf, lightEngineConfig()));
        return step;
    }

    private TransformationStepConfig sortProfile() {
        TransformationStepConfig step = new TransformationStepConfig();
        List<Integer> inputSteps = Collections.singletonList(profileStep);
        step.setInputSteps(inputSteps);
        step.setTransformer(TRANSFORMER_SORTER);

        SorterConfig conf = new SorterConfig();
        conf.setPartitions(1);
        conf.setCompressResult(true);
        conf.setSortingField(DataCloudConstants.PROFILE_ATTR_ATTRNAME);
        String confStr = appendEngineConf(conf, lightEngineConfig());
        step.setConfiguration(confStr);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(profileTablePrefix);
        step.setTargetTable(targetTable);

        return step;
    }

    @Override
    protected void enrichTableSchema(Table servingStoreTable) {
        List<Attribute> attributes = servingStoreTable.getAttributes();

        for (Attribute attribute : attributes) {
            attribute.setCategory(Category.PRODUCT_SPEND.getName());

            if (!InterfaceName.AccountId.name().equalsIgnoreCase(attribute.getName())) {
                String productId = ActivityMetricsUtils.getProductIdFromFullName(attribute.getName());
                if (StringUtils.isBlank(productId)) {
                    throw new RuntimeException("Cannot parse product id from attribute name " + attribute.getName());
                }

                String productName = null;
                List<Product> products = productMap.get(productId);
                if (products != null) {
                    for (Product product : products) {
                        productName = product.getProductName();
                        if (productName != null) {
                            break;
                        }
                    }
                }
                if (productName == null) {
                    productName = productId;
                }
                if (StringUtils.isBlank(productName)) {
                    throw new IllegalArgumentException("Cannot find product name for product id " + productId
                            + " in product map " + JsonUtils.serialize(productMap));
                }

                Pair<String, String> displayNames = ActivityMetricsUtils
                        .getDisplayNamesFromFullName(attribute.getName(), evaluationDate, periodStrategies);
                if (ActivityMetricsUtils.isDeprecated(attribute.getName(), purchaseMetrics)) {
                    attribute.setDisplayName(displayNames.getLeft() + " (Deprecated)");
                } else {
                    attribute.setDisplayName(displayNames.getLeft());
                }
                attribute.setSecondaryDisplayName(displayNames.getRight());
                attribute.setSubcategory(productName);
                if (ActivityMetricsUtils.isHasPurchasedAttr(attribute.getName())) {
                    attribute.setFundamentalType(FundamentalType.BOOLEAN);
                } else {
                    attribute.setFundamentalType(FundamentalType.NUMERIC);
                }
                attribute.setDescription(ActivityMetricsUtils.getDescriptionFromFullName(attribute.getName()));
            }
            attribute.removeAllowedDisplayNames();
        }
    }

    private ActivityMetrics createHasPurchasedMetrics() {
        Tenant tenant = MultiTenantContext.getTenant();
        ActivityMetrics metrics = new ActivityMetrics();
        metrics.setMetrics(InterfaceName.HasPurchased);
        metrics.setPeriodsConfig(Arrays.asList(TimeFilter.ever()));
        metrics.setType(ActivityType.PurchaseHistory);
        metrics.setTenant(tenant);
        metrics.setEOL(false);
        metrics.setDeprecated(null);
        metrics.setCreated(new Date());
        metrics.setUpdated(metrics.getCreated());
        return metrics;
    }

    private void generateReport() {
        ObjectNode report = getObjectFromContext(ReportPurpose.PROCESS_ANALYZE_RECORDS_SUMMARY.getKey(),
                ObjectNode.class);
        updateReportPayload(report);
        putObjectInContext(ReportPurpose.PROCESS_ANALYZE_RECORDS_SUMMARY.getKey(), report);
    }

    private void updateReportPayload(ObjectNode report) {
        try {
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

            ObjectMapper mapper = new ObjectMapper();
            ArrayNode actionNode = mapper.createArrayNode();
            List<Action> actions = actionProxy.getActionsByPids(customerSpace.toString(), configuration.getActionIds());
            SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy hh:mm a");
            if (actions != null) {
                actions.forEach(action -> {
                    if (action.getType() == ActionType.ACTIVITY_METRICS_CHANGE) {
                        ObjectNode on = mapper.createObjectNode();
                        on.put(ReportConstants.TIME, sdf.format(action.getCreated()));
                        on.put(ReportConstants.ACTION, JsonUtils.serialize(action.getActionConfiguration()));
                        on.put(ReportConstants.USER, action.getActionInitiator());
                        actionNode.add(on);
                    }
                });
            }
            ((ObjectNode) consolidateSummaryNode).set(ReportConstants.ACTIONS, actionNode);

            JsonNode entityStatsSummaryNode = entityNode.get(ReportPurpose.ENTITY_STATS_SUMMARY.getKey());
            if (entityStatsSummaryNode == null) {
                entityStatsSummaryNode = ((ObjectNode) entityNode)
                        .putObject(ReportPurpose.ENTITY_STATS_SUMMARY.getKey());
            }
            ((ObjectNode) entityStatsSummaryNode).put(ReportConstants.TOTAL, String.valueOf(purchaseMetrics.size()));
        } catch (Exception e) {
            throw new RuntimeException("Fail to update report payload", e);
        }
    }
}
