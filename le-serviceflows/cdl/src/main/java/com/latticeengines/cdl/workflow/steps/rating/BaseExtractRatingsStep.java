package com.latticeengines.cdl.workflow.steps.rating;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.GenerateRatingStepConfiguration;
import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.MergeScoringTargetsConfig;
import com.latticeengines.serviceflows.workflow.util.ScalingUtils;
import com.latticeengines.spark.exposed.job.cdl.MergeScoringTargets;

public abstract class BaseExtractRatingsStep<T extends GenerateRatingStepConfiguration> extends BaseEventQueryStep<T> {

    private static final Logger log = LoggerFactory.getLogger(BaseExtractRatingsStep.class);

    protected List<RatingModelContainer> containers;
    private List<RatingModelContainer> round;

    protected abstract List<RatingEngineType> getTargetEngineTypes();
    protected abstract HdfsDataUnit extractTargets(RatingModelContainer container);
    protected abstract boolean isRuleBased();
    protected abstract GenericRecord getDummyRecord();

    protected void setupExtractStep() {
        super.setupQueryStep();
        List<RatingModelContainer> allContainers = getListObjectFromContext(ITERATION_RATING_MODELS,
                RatingModelContainer.class);
        containers = allContainers.stream() //
                .filter(container -> getTargetEngineTypes().contains(container.getEngineSummary().getType())) //
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(containers)) {
            throw new IllegalStateException("There is no models of type " + getTargetEngineTypes() + " to be rated.");
        }
    }

    void extractAllContainers() {
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        retry.execute(ctx -> {
            if (ctx.getRetryCount() > 0) {
                log.info("(Attempt=" + (ctx.getRetryCount() + 1) + ") extract rating containers via Spark SQL.");
                Throwable t = ctx.getLastThrowable();
                if (t != null) {
                    log.warn("Spark script failed.", t);
                }
            }
            do {
                round = containers.stream() //
                        .filter(container -> container.getExtractedTarget() == null) //
                        .limit(16) //
                        .collect(Collectors.toList());
                if (CollectionUtils.isNotEmpty(round)) {
                    try {
                        boolean persistOnDisk = getTotalRatingWeights() > 8;
                        startSparkSQLSession(getHdfsPaths(attrRepo), persistOnDisk);
                        prepareEventQuery(persistOnDisk);
                        round.forEach(this::extractOneContainer);
                    } finally {
                        stopSparkSQLSession();
                    }
                }
            } while (CollectionUtils.isNotEmpty(round));
            return true;
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
        computeScalingMultiplier(input, 1);

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
        int scalingByWeights = (int) Math.floor(ratingWeights / 32.) + 1;
        int maxMultiplier = ScalingUtils.getMultiplier(1000);
        int scalingFactor = Math.min(maxMultiplier, scalingByWeights * ScalingUtils.getMultiplier(totalSizeInGb));
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
                    case RULE_BASED:
                        weights.addAndGet(4);
                        break;
                    default:
                        weights.addAndGet(1);
                }
            });
        }
        return weights.get();
    }

}
