package com.latticeengines.cdl.workflow.steps.rating;

import static com.latticeengines.workflow.exposed.build.WorkflowStaticContext.ATTRIBUTE_REPO;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;

import com.latticeengines.cdl.workflow.steps.export.BaseSparkSQLStep;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.GenerateRatingStepConfiguration;
import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.MergeScoringTargetsConfig;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;
import com.latticeengines.serviceflows.workflow.util.ScalingUtils;
import com.latticeengines.spark.exposed.job.cdl.MergeScoringTargets;
import com.latticeengines.workflow.exposed.build.WorkflowStaticContext;

public abstract class BaseExtractRatingsStep<T extends GenerateRatingStepConfiguration> extends BaseSparkSQLStep<T> {

    private static final Logger log = LoggerFactory.getLogger(BaseExtractRatingsStep.class);

    @Inject
    private PeriodProxy periodProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    protected DataCollection.Version version;
    private String evaluationDate;
    private AttributeRepository attrRepo;
    protected List<RatingModelContainer> containers;
    private List<RatingModelContainer> round;

    protected abstract List<RatingEngineType> getTargetEngineTypes();
    protected abstract HdfsDataUnit extractTargets(RatingModelContainer container);
    protected abstract boolean isRuleBased();
    protected abstract GenericRecord getDummyRecord();

    void setupExtractStep() {
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
            evaluationDate = getStringValueFromContext(CDL_EVALUATION_DATE);
            if (StringUtils.isBlank(evaluationDate)) {
                evaluationDate = periodProxy.getEvaluationDate(parseCustomerSpace(stepConfiguration).toString());
            }
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
        return dataCollectionProxy.getAttrRepo(customerSpace.toString(), version);
    }

    void extractAllContainers() {
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        retry.execute(ctx -> {
            if (ctx.getRetryCount() > 0) {
                log.info("(Attempt=" + (ctx.getRetryCount() + 1) + ") extract rating containers via Spark SQL.");
            }
            try {
                round = containers.stream() //
                        .filter(container -> container.getExtractedTarget() == null) //
                        .collect(Collectors.toList());
                if (CollectionUtils.isNotEmpty(round)) {
                    boolean persistOnDisk = getTotalRatingWeights() > 8;
                    startSparkSQLSession(getHdfsPaths(attrRepo), persistOnDisk);
                    round.forEach(this::extractOneContainer);
                }
                return true;
            } finally {
                stopSparkSQLSession();
            }
        });
    }

    private void extractOneContainer(RatingModelContainer container) {
        RatingEngineSummary engineSummary = container.getEngineSummary();
        RatingEngineType engineType = engineSummary.getType();
        log.info("Extracting scoring target for " + engineType + " rating engine " + engineSummary.getId());
        HdfsDataUnit result = extractTargets(container);
        if (result == null) {
            throw new RuntimeException("Cannot extract scoring target from rating model " //
                    + container.getModel().getId());
        }
        boolean isDummry = writeDummyRecordIfNecessary(container, result);
        container.setExtractedTarget(result);
    }

    void mergeResults(String resultTableName) {
        List<DataUnit> input = containers.stream() //
                .map(RatingModelContainer::getExtractedTarget).collect(Collectors.toList());
        MergeScoringTargetsConfig jobConfig = new MergeScoringTargetsConfig();
        jobConfig.setInput(input);
        jobConfig.setContainers(containers);
        jobConfig.setWorkspace(getRandomWorkspace());
        jobConfig.setRuleBased(isRuleBased());
        computeScalingMultiplier(input);

        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        SparkJobResult result = retry.execute(ctx -> {
            if (ctx.getRetryCount() > 0) {
                log.info("(Attempt=" + (ctx.getRetryCount() + 1) + ") merging " //
                        + containers.size() + " scoring targets.");
                log.warn("Previous failure:",  ctx.getLastThrowable());
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

        String pk = InterfaceName.__Composite_Key__.name();
        Table resultTable = toTable(resultTableName, pk, result.getTargets().get(0));
        metadataProxy.createTable(customerSpace.toString(), resultTableName, resultTable);
    }

    private boolean writeDummyRecordIfNecessary(RatingModelContainer container, HdfsDataUnit result) {
        long count = result.getCount();
        if (count == 0) {
            String engineId = container.getEngineSummary().getId();
            RetryTemplate retry = RetryUtils.getRetryTemplate(3);
            try {
                return retry.execute(ctx -> {
                    String resultDir = PathUtils.toParquetOrAvroDir(result.getPath());
                    if (HdfsUtils.fileExists(yarnConfiguration, resultDir)) {
                        HdfsUtils.rmdir(yarnConfiguration, resultDir);
                    }
                    String avroPath = resultDir + "/part-" + UUID.randomUUID().toString() + ".avro";
                    log.info("(Attempt=" + ctx.getRetryCount() + ") write dummy record for " + engineId //
                            + " at " + avroPath);
                    GenericRecord record = getDummyRecord();
                    AvroUtils.writeToHdfsFile(yarnConfiguration, record.getSchema(), avroPath, //
                            Collections.singletonList(record), true);
                    return true;
                });
            } catch (Exception e) {
                throw new RuntimeException("Failed to write dummy record.", e);
            }
        } else {
            return false;
        }
    }

    protected int scaleBySize(double totalSizeInGb) {
        int ratingWeights = getTotalRatingWeights();
        int scalingByWeights = (int) Math.floor(ratingWeights / 40.) + 1;
        int scalingFactor = Math.min(5, scalingByWeights * ScalingUtils.getMultiplier(totalSizeInGb));
        if (scalingFactor > 1) {
            log.info("Adjust scaling factor to " + scalingFactor + " based on total size " + totalSizeInGb //
                    + " gb and rating weights " + ratingWeights);
        }
        return scalingFactor;
    }

    private int getTotalRatingWeights() {
        AtomicInteger weights = new AtomicInteger(0);
        if (CollectionUtils.isNotEmpty(round)) {
            round.forEach(container -> {
                switch (container.getEngineSummary().getType()) {
                    case CROSS_SELL:
                        weights.addAndGet(4);
                        break;
                    case RULE_BASED:
                        weights.addAndGet(2);
                        break;
                    default:
                        weights.addAndGet(1);
                }
            });
        }
        return weights.get();
    }

}
