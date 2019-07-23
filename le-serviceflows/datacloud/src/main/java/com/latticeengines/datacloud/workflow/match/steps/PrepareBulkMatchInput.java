package com.latticeengines.datacloud.workflow.match.steps;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.match.exposed.service.MatchCommandService;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudJobConfiguration;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchStatus;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps.PrepareBulkMatchInputConfiguration;
import com.latticeengines.serviceflows.workflow.util.SparkUtils;
import com.latticeengines.spark.exposed.service.LivySessionService;
import com.latticeengines.spark.exposed.service.SparkJobService;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("prepareBulkMatchInput")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PrepareBulkMatchInput extends BaseWorkflowStep<PrepareBulkMatchInputConfiguration> {

    private static Logger log = LoggerFactory.getLogger(PrepareBulkMatchInput.class);
    private Schema schema;

    @Inject
    private MatchCommandService matchCommandService;

    @Inject
    private HdfsPathBuilder hdfsPathBuilder;

    @Inject
    private LivySessionService sessionService;

    @Inject
    private SparkJobService sparkJobService;

    @Value("${datacloud.match.fetch.concurrent.blocks.max}")
    private Integer maxFetchConcurrentBlocks;

    // After we introduce customized entity, need to have default setting for
    // maximum concurrent block number
    @Value("${datacloud.match.account.concurrent.blocks.max}")
    private Integer maxAccountConcurrentBlocks;

    @Value("${datacloud.match.contact.concurrent.blocks.max}")
    private Integer maxContactConcurrentBlocks;

    @Value("${datacloud.match.txn.concurrent.blocks.max}")
    private Integer maxTxnConcurrentBlocks;

    @Value("${datacloud.match.fuzzy.block.size.min}")
    private Integer minFuzzyBlockSize;

    @Value("${datacloud.match.fuzzy.block.size.max}")
    private Integer maxFuzzyBlockSize;

    @Value("${datacloud.match.fetch.block.size.min}")
    private Integer minFetchBlockSize;

    @Value("${datacloud.match.fetch.block.size.max}")
    private Integer maxFetchBlockSize;

    private String avroGlobs;

    @Override
    public void execute() {
        log.info("Inside PrepareBulkMatchInput execute()");
        String avroDir = getConfiguration().getInputAvroDir();
        HdfsPodContext.changeHdfsPodId(getConfiguration().getHdfsPodId());

        MatchInput input = getConfiguration().getMatchInput();
        if (getConfiguration().getRootOperationUid() == null) {
            String rootOperationUid = UUID.randomUUID().toString();
            input.setRootOperationUid(rootOperationUid);
            getConfiguration().setRootOperationUid(rootOperationUid);
            log.info("Assign root operation uid " + rootOperationUid + " to match input.");
            matchCommandService.start(input, null, rootOperationUid);
        }

        if (matchCommandService.getByRootOperationUid(getConfiguration().getRootOperationUid()) == null) {
            log.info("Insert new match command for root uid " + getConfiguration().getRootOperationUid());
            matchCommandService.start(input, null, getConfiguration().getRootOperationUid());
        }
        putStringValueInContext(BulkMatchContextKey.ROOT_OPERATION_UID, getConfiguration().getRootOperationUid());

        avroGlobs = MatchUtils.toAvroGlobs(avroDir);
        Long count = SparkUtils.countRecordsInGlobs(sessionService, sparkJobService, yarnConfiguration, avroGlobs);
        schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, avroGlobs);
        Integer[] blocks = determineBlockSizes(count);
        List<DataCloudJobConfiguration> configurations = readAndSplitInputAvro(blocks);

        executionContext.put(BulkMatchContextKey.YARN_JOB_CONFIGS, configurations);
        matchCommandService.update(getConfiguration().getRootOperationUid()) //
                .status(MatchStatus.MATCHING) //
                .rowsRequested(count.intValue()) //
                .progress(0.05f) //
                .commit();

        log.info("Execution Context=" + JsonUtils.serialize(executionContext));
    }

    @VisibleForTesting
    Integer[] determineBlockSizes(Long count) {
        if (MatchUtils.isValidForAccountMasterBasedMatch(getConfiguration().getMatchInput().getDataCloudVersion())) {
            if (getConfiguration().getMatchInput().isFetchOnly()) {
                return divideIntoNumBlocks(count, determineNumBlocksForFetchOnly(count));
            } else {
                return divideIntoNumBlocks(count,
                        determineNumBlocksForFuzzyMatch(count, getConfiguration().getMatchInput()));
            }
        } else {
            return divideIntoNumBlocks(count, determineNumBlocksForRTS(count));
        }
    }

    /**
     * For 2.0 DataCloud based fuzzy match (Non-FetchOnly mode)
     *
     * Determine total number of blocks and set maximum concurrent #block into executionContext
     *
     * @param count
     * @param input
     * @return
     */
    private Integer determineNumBlocksForFuzzyMatch(Long count, MatchInput input) {
        // Entity -> (minBlockSize, maxBlockSize, maxConcurrentBlocks)
        @SuppressWarnings("serial")
        Map<String, Triple<Integer, Integer, Integer>> entityBlockInfo = new HashMap<String, Triple<Integer, Integer, Integer>>() {
            {
                // LDC Match
                put(null, Triple.of(minFuzzyBlockSize, maxFuzzyBlockSize, maxAccountConcurrentBlocks)); //
                // LDC Match
                put(BusinessEntity.LatticeAccount.name(),
                        Triple.of(minFuzzyBlockSize, maxFuzzyBlockSize, maxAccountConcurrentBlocks)); //
                // Account Entity Match
                put(BusinessEntity.Account.name(),
                        Triple.of(minFuzzyBlockSize, maxFuzzyBlockSize, maxAccountConcurrentBlocks)); //
                // Contact Entity Match (M28)
                put(BusinessEntity.Contact.name(),
                        Triple.of(minFuzzyBlockSize, maxFuzzyBlockSize, maxContactConcurrentBlocks)); //
                // Transaction Entity Match (M29)
                put(BusinessEntity.Transaction.name(),
                        Triple.of(minFuzzyBlockSize, maxFuzzyBlockSize, maxTxnConcurrentBlocks)); //
            }
        };

        if (!entityBlockInfo.containsKey(input.getTargetEntity())) {
            throw new UnsupportedOperationException("Unsupported target entity in match: " + input.getTargetEntity());
        }
        Triple<Integer, Integer, Integer> blockInfo = entityBlockInfo.get(input.getTargetEntity());
        // Fail fast in case we configure anything wrong
        if (ObjectUtils.defaultIfNull(blockInfo.getLeft(), 0) == 0
                || ObjectUtils.defaultIfNull(blockInfo.getMiddle(), 0) == 0
                || ObjectUtils.defaultIfNull(blockInfo.getRight(), 0) == 0) {
            throw new IllegalArgumentException(
                    String.format("Invalid setting for entity %s: %s", input.getTargetEntity(), blockInfo.toString()));
        }
        executionContext.put(BulkMatchContextKey.MAX_CONCURRENT_BLOCKS, blockInfo.getRight());
        return determineNumBlocks(count, blockInfo.getLeft(), blockInfo.getMiddle(), blockInfo.getRight());
    }

    /**
     * For 2.0 DataCloud based fuzzy match (FetchOnly mode)
     *
     * Determine total number of blocks and set maximum concurrent #block into executionContext
     *
     * @param count
     * @return
     */
    private Integer determineNumBlocksForFetchOnly(Long count) {
        executionContext.put(BulkMatchContextKey.MAX_CONCURRENT_BLOCKS, maxFetchConcurrentBlocks);
        return determineNumBlocks(count, minFetchBlockSize, maxFetchBlockSize, maxFetchConcurrentBlocks);
    }

    /**
     * For V1.0 DerivedColumnsCache based SQL lookup match
     *
     * Determine total number of blocks and set maximum concurrent #block into executionContext
     *
     * @param count
     * @return
     */
    private Integer determineNumBlocksForRTS(Long count) {
        Integer numBlocks = 1;
        Integer averageBlockSize = getConfiguration().getAverageBlockSize();
        while (count >= averageBlockSize * numBlocks && numBlocks < maxFetchConcurrentBlocks) {
            numBlocks++;
        }
        executionContext.put(BulkMatchContextKey.MAX_CONCURRENT_BLOCKS, maxFetchConcurrentBlocks);
        return numBlocks;
    }

    private Integer determineNumBlocks(Long count, Integer minBlockSize, Integer maxBlockSize,
            Integer maxConcurrentBlocks) {
        Integer numBlocks;
        if (count < ((long) minBlockSize * maxConcurrentBlocks)) {
            numBlocks = Math.max((int) (count / minBlockSize), 1);
        } else if (count > ((long) maxBlockSize * maxConcurrentBlocks)) {
            numBlocks = (int) (count / maxBlockSize);
        } else {
            numBlocks = maxConcurrentBlocks;
        }
        return numBlocks;
    }

    private Integer[] divideIntoNumBlocks(Long count, Integer numBlocks) {
        long blockSize = count / numBlocks;
        Integer[] blocks = new Integer[numBlocks];
        Long sum = 0L;
        for (int i = 0; i < numBlocks - 1; i++) {
            blocks[i] = (int) blockSize;
            sum += (int) blockSize;
        }
        blocks[numBlocks - 1] = Long.valueOf(count - sum).intValue();
        log.info("Divide input into blocks [" + StringUtils.join(blocks, ", ") + "]");
        return blocks;
    }

    private List<DataCloudJobConfiguration> readAndSplitInputAvro(Integer[] blocks) {
        Iterator<GenericRecord> iterator = AvroUtils.iterateAvroFiles(yarnConfiguration, avroGlobs);
        List<DataCloudJobConfiguration> configurations = new ArrayList<>();

        int blockIdx = 0;
        for (Integer blockSize : blocks) {
            blockIdx++;
            String blockOperationUid = UUID.randomUUID().toString().toUpperCase();

            DataCloudJobConfiguration jobConfiguration = generateJobConfiguration();
            jobConfiguration.setBlockSize(blockSize);
            jobConfiguration.setBlockOperationUid(blockOperationUid);
            if (blocks.length == 1) {
                jobConfiguration.setAvroPath(avroGlobs);
            } else {
                String targetFile = hdfsPathBuilder.constructMatchBlockInputAvro(jobConfiguration.getRootOperationUid(),
                        jobConfiguration.getBlockOperationUid()).toString();
                jobConfiguration.setAvroPath(targetFile);
                writeBlock(iterator, blockSize, targetFile);
            }
            jobConfiguration.setInputAvroSchema(getConfiguration().getInputAvroSchema());
            String appId = matchCommandService.getByRootOperationUid(getConfiguration().getRootOperationUid())
                    .getApplicationId();
            if (StringUtils.isBlank(appId)) {
                jobConfiguration.setAppName(String.format("%s~DataCloudMatch~Block[%d/%d]",
                        getConfiguration().getCustomerSpace().getTenantId(), blockIdx, blocks.length));
            } else {
                jobConfiguration.setAppName(String.format("%s~DataCloudMatch[%s]~Block[%d/%d]",
                        getConfiguration().getCustomerSpace().getTenantId(), appId, blockIdx, blocks.length));
            }
            writeMatchInput(jobConfiguration);
            configurations.add(jobConfiguration);
        }

        return configurations;
    }

    private void writeMatchInput(DataCloudJobConfiguration jobConfiguration) {
        String avroPath = jobConfiguration.getAvroPath();
        String matchInputFile = FilenameUtils.getFullPath(avroPath) + "MatchInput_"
                + jobConfiguration.getRootOperationUid() + ".json";
        try {
            MatchInput matchInput = jobConfiguration.getMatchInput();
            if (matchInput.getInputBuffer() != null) {
                RetryTemplate retry = RetryUtils.getRetryTemplate(3);
                retry.execute(ctx ->  {
                    if (ctx.getRetryCount() > 0) {
                        log.info("(Attempt=" + ctx.getRetryCount() + ") writing MatchInput on to hdfs, " + //
                                "path=" + matchInputFile);
                    }
                    try {
                        if (HdfsUtils.fileExists(yarnConfiguration, matchInputFile)) {
                            HdfsUtils.rmdir(yarnConfiguration, matchInputFile);
                        }
                        HdfsUtils.writeToFile(yarnConfiguration, matchInputFile, JsonUtils.serialize(matchInput));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    log.info("Wrote MatchInput on to hdfs, path=" + matchInputFile);
                    return 0;
                });
            }
            jobConfiguration.setMatchInputPath(matchInputFile);
            matchInput.setInputBuffer(null);
            matchInput.setCustomSelection(null);
            matchInput.setPredefinedSelection(null);
            matchInput.setUnionSelection(null);
            matchInput.setFields(null);
            matchInput.setKeyMap(null);

        } catch (Exception e) {
            log.warn("Can not write MatchInput on hdfs, path=" + matchInputFile, e);
        }

    }

    private void writeBlock(Iterator<GenericRecord> iterator, Integer blockSize, String targetFile) {
        String localFile = targetFile.substring(targetFile.lastIndexOf("/") + 1);
        FileUtils.deleteQuietly(new File(localFile));
        int bufferSize = 2000;
        List<GenericRecord> data = new ArrayList<>();
        int count = 0;
        while (count < blockSize && iterator.hasNext()) {
            data.add(iterator.next());
            count++;
            if (data.size() >= bufferSize) {
                writeBuffer(localFile, data);
                data.clear();
            }
        }
        if (data.size() > 0) {
            writeBuffer(localFile, data);
        }
        log.info("Write a block of " + count + " rows to " + localFile);
        uploadBlockInput(localFile, targetFile);
    }

    private void writeBuffer(String localFile, List<GenericRecord> data) {
        try {
            if (new File(localFile).exists()) {
                AvroUtils.appendToLocalFile(data, localFile, true);
            } else {
                AvroUtils.writeToLocalFile(schema, data, localFile, true);
            }
            log.info("Write a buffer of " + data.size() + " rows to " + localFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void uploadBlockInput(String localFile, String targetFile) {
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        retry.execute(ctx -> {
            if (ctx.getRetryCount() > 0) {
                log.info("(Attempt=" + ctx.getRetryCount() + ") uploading " + localFile + " to " + targetFile);
            }
            try {
                if (HdfsUtils.fileExists(yarnConfiguration, targetFile)) {
                    HdfsUtils.rmdir(yarnConfiguration, targetFile);
                }
                HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, localFile, targetFile);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            log.info("Uploaded " + localFile + " to " + targetFile);
            return 0;
        });
    }

    private DataCloudJobConfiguration generateJobConfiguration() {
        DataCloudJobConfiguration jobConfiguration = new DataCloudJobConfiguration();
        jobConfiguration.setHdfsPodId(getConfiguration().getHdfsPodId());
        jobConfiguration.setMatchInput(getConfiguration().getMatchInput());
        jobConfiguration.setName("DataCloudMatchBlock");
        jobConfiguration.setCustomerSpace(getConfiguration().getCustomerSpace());
        jobConfiguration.setRootOperationUid(getConfiguration().getRootOperationUid());
        jobConfiguration.setYarnQueue(getConfiguration().getYarnQueue());
        if (Boolean.TRUE.equals(getConfiguration().getMatchInput().getUseRealTimeProxy())) {
            jobConfiguration.setRealTimeProxyUrl(getConfiguration().getRealTimeProxyUrl());
            jobConfiguration.setThreadPoolSize(getConfiguration().getRealTimeThreadPoolSize());
        }
        return jobConfiguration;
    }

}
