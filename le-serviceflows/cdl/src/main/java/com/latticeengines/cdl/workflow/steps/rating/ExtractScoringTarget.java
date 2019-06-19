package com.latticeengines.cdl.workflow.steps.rating;

import static com.latticeengines.workflow.exposed.build.WorkflowStaticContext.ATTRIBUTE_REPO;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.export.BaseSparkSQLStep;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ModelingQueryType;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.GenerateRatingStepConfiguration;
import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.MergeScoringTargetsConfig;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.spark.exposed.job.cdl.MergeScoringTargets;
import com.latticeengines.workflow.exposed.build.WorkflowStaticContext;

@Component("extractScoringTarget")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExtractScoringTarget extends BaseSparkSQLStep<GenerateRatingStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ExtractScoringTarget.class);

    @Inject
    private PeriodProxy periodProxy;

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    private DataCollection.Version version;
    private String evaluationDate;
    private AttributeRepository attrRepo;
    protected List<RatingModelContainer> containers;
    private Map<String, RatingEngineType> ratingEngineTypeMap = new HashMap<>();
    private boolean hasCrossSellModel = false;

    @Override
    public void execute() {
        customerSpace = parseCustomerSpace(configuration);
        version = parseDataCollectionVersion(configuration);
        attrRepo = parseAttrRepo(configuration);
        evaluationDate = parseEvaluationDateStr(configuration);

        List<RatingModelContainer> allContainers = getListObjectFromContext(ITERATION_RATING_MODELS,
                RatingModelContainer.class);
        containers = allContainers.stream() //
                .filter(container -> getTargetEngineTypes().contains(container.getEngineSummary().getType())) //
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(containers)) {
            throw new IllegalStateException("There is no models of type " + getTargetEngineTypes() + " to be rated.");
        }
        containers.forEach(container -> {
            String modelGuid = ((AIModel) container.getModel()).getModelSummaryId();
            RatingEngineType ratingEngineType = container.getEngineSummary().getType();
            ratingEngineTypeMap.put(modelGuid, ratingEngineType);
            if (!hasCrossSellModel && RatingEngineType.CROSS_SELL.equals(ratingEngineType)) {
                hasCrossSellModel = true;
            }
        });

        extractAllContainers();
        String resultTableName = mergeResults();

        removeObjectFromContext(FILTER_EVENT_TABLE);
        putStringValueInContext(FILTER_EVENT_TARGET_TABLE_NAME, resultTableName);
        putStringValueInContext(SCORING_UNIQUEKEY_COLUMN, InterfaceName.__Composite_Key__.name());
        putStringValueInContext(HAS_CROSS_SELL_MODEL, String.valueOf(hasCrossSellModel));
    }

    @Override
    protected CustomerSpace parseCustomerSpace(GenerateRatingStepConfiguration stepConfiguration) {
        if (customerSpace == null) {
            customerSpace = configuration.getCustomerSpace();
        }
        return customerSpace;
    }

    @Override
    protected DataCollection.Version parseDataCollectionVersion(GenerateRatingStepConfiguration stepConfiguration) {
        if (version == null) {
            version = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
            if (version == null) {
                version = configuration.getDataCollectionVersion();
            }
            log.info("Using data collection version " + version);
        }
        return version;
    }

    @Override
    protected String parseEvaluationDateStr(GenerateRatingStepConfiguration stepConfiguration) {
        if (StringUtils.isBlank(evaluationDate)) {
            evaluationDate = periodProxy.getEvaluationDate(parseCustomerSpace(stepConfiguration).toString());
        }
        return evaluationDate;
    }

    @Override
    protected AttributeRepository parseAttrRepo(GenerateRatingStepConfiguration stepConfiguration) {
        AttributeRepository attrRepo = WorkflowStaticContext.getObject(ATTRIBUTE_REPO, AttributeRepository.class);
        if (attrRepo == null) {
            attrRepo = constructAttrRepo();
            WorkflowStaticContext.putObject(ATTRIBUTE_REPO, attrRepo);
        }
        return attrRepo;
    }

    private AttributeRepository constructAttrRepo() {
        AttributeRepository attrRepo = dataCollectionProxy.getAttrRepo(customerSpace.toString(), version);
        Table accountExportTable = dataCollectionProxy.getTable(customerSpace.toString(), //
                TableRoleInCollection.AccountExport, version);
        if (accountExportTable != null) {
            log.info("Overwriting account attr repo by account export table");
            attrRepo.appendServingStore(BusinessEntity.Account, accountExportTable);
        }

        Table purchaseHistoryTable = dataCollectionProxy.getTable(customerSpace.toString(), //
                TableRoleInCollection.CalculatedPurchaseHistory, version);
        if (purchaseHistoryTable != null) {
            log.info("Insert PH attr repo");
            attrRepo.appendServingStore(BusinessEntity.PurchaseHistory, purchaseHistoryTable);
        }
        return attrRepo;
    }

    private List<RatingEngineType> getTargetEngineTypes() {
        return Arrays.asList(RatingEngineType.CROSS_SELL, RatingEngineType.CUSTOM_EVENT);
    }

    private void extractAllContainers() {
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        retry.execute(ctx -> {
            if (ctx.getRetryCount() > 0) {
                log.info("(Attempt=" + (ctx.getRetryCount() + 1) + ") extract rating containers via Spark SQL.");
            }
            try {
                startSparkSQLSession(getHdfsPaths(attrRepo));
                containers.forEach(container -> {
                    if (container.getExtractedTarget() == null) {
                        extractOneContainer(container);
                    }
                });
                return true;
            } finally {
                stopSparkSQLSession();
            }
        });
    }

    private String mergeResults() {
        List<DataUnit> input = containers.stream() //
                .map(RatingModelContainer::getExtractedTarget).collect(Collectors.toList());
        MergeScoringTargetsConfig jobConfig = new MergeScoringTargetsConfig();
        jobConfig.setInput(input);
        jobConfig.setContainers(containers);
        jobConfig.setWorkspace(getRandomWorkspace());
        computeScalingMultiplier(input);

        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        SparkJobResult result = retry.execute(ctx -> {
            if (ctx.getRetryCount() > 0) {
                log.info("(Attempt=" + (ctx.getRetryCount() + 1) + ") merging " //
                        + containers.size() + " scoring targets.");
            }
            try {
                String tenantId = CustomerSpace.shortenCustomerSpace(customerSpace.toString());
                String jobName = tenantId + "~" + MergeScoringTargets.class.getSimpleName() + "~" + getClass().getSimpleName();
                LivySession session = createLivySession(jobName);
                return runSparkJob(session, MergeScoringTargets.class, jobConfig);
            } finally {
                killLivySession();
            }
        });

        String resultTableName = NamingUtils.timestamp("ScoringTarget");
        String pk = InterfaceName.__Composite_Key__.name();
        Table resultTable = toTable(resultTableName, pk, result.getTargets().get(0));
        metadataProxy.createTable(customerSpace.toString(), resultTableName, resultTable);

        return resultTableName;
    }

    private void extractOneContainer(RatingModelContainer container) {
        RatingEngineSummary engineSummary = container.getEngineSummary();
        RatingEngineType engineType = engineSummary.getType();
        HdfsDataUnit result = null;
        log.info("Extracting scoring target for " + engineType + " rating engine " + engineSummary.getId());
        if (RatingEngineType.CUSTOM_EVENT.equals(engineType)) {
            FrontEndQuery frontEndQuery = customEventQuery(engineSummary);
            result = getEntityQueryData(frontEndQuery);
        } else if (RatingEngineType.CROSS_SELL.equals(engineType)) {
            EventFrontEndQuery frontEndQuery = crossSellQuery(engineSummary.getId(), container.getModel().getId());
            result = getEventScoringTarget(frontEndQuery);
        }
        if (result == null) {
            throw new RuntimeException("Cannot extract scoring target from rating model " //
                    + container.getModel().getId());
        }
        boolean isDummry = writeDummyRecordIfNecessary(container, result);
        container.setExtractedTarget(result);
    }

    private boolean writeDummyRecordIfNecessary(RatingModelContainer container, HdfsDataUnit result) {
        long count = result.getCount();
        if (count == 0) {
            String engineId = container.getEngineSummary().getId();
            log.info("Engine " + engineId + " has empty scoring target, writing a dummy record.");
            String avroGlob = PathUtils.toAvroGlob(result.getPath());
            Schema schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, avroGlob);
            RetryTemplate retry = RetryUtils.getRetryTemplate(3);
            try {
                return retry.execute(ctx -> {
                    if (ctx.getRetryCount() > 0) {
                        log.info("(Attemp=" + ctx.getRetryCount() + ") write dummy record for " + engineId);
                    }
                    String avroPath = HdfsUtils.getFilesByGlob(yarnConfiguration, avroGlob).get(0);
                    if (HdfsUtils.fileExists(yarnConfiguration, avroPath)) {
                        HdfsUtils.rmdir(yarnConfiguration, avroPath);
                    }
                    GenericRecordBuilder builder = new GenericRecordBuilder(schema);
                    String accountId = NamingUtils.uuid("DummyAccount");
                    builder.set(InterfaceName.AccountId.name(), accountId);
                    builder.set(InterfaceName.PeriodId.name(), 0L);
                    GenericRecord record = builder.build();
                    AvroUtils.writeToHdfsFile(yarnConfiguration, schema, avroPath, Collections.singletonList(record), true);
                    return true;
                });
            } catch (Exception e) {
                throw new RuntimeException("Failed to write dummy record.", e);
            }
        } else {
            return false;
        }
    }

    private FrontEndQuery customEventQuery(RatingEngineSummary engineSummary) {
        MetadataSegment segment = segmentProxy.getMetadataSegmentByName(customerSpace.toString(), //
                engineSummary.getSegmentName());
        AttributeLookup accountId = new AttributeLookup(BusinessEntity.Account, InterfaceName.AccountId.name());
        FrontEndQuery frontEndQuery = segment.toFrontEndQuery(BusinessEntity.Account);
        frontEndQuery.setLookups(Collections.singletonList(accountId));
        return frontEndQuery;
    }

    private EventFrontEndQuery crossSellQuery(String engineId, String modelId) {
        return ratingEngineProxy.getModelingQueryByRatingId(customerSpace.toString(), engineId, modelId, //
                ModelingQueryType.TARGET);
    }

}
