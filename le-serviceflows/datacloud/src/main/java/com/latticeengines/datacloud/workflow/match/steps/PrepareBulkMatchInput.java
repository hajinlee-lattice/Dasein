package com.latticeengines.datacloud.workflow.match.steps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.match.exposed.service.MatchCommandService;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudJobConfiguration;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchStatus;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps.PrepareBulkMatchInputConfiguration;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("prepareBulkMatchInput")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PrepareBulkMatchInput extends BaseWorkflowStep<PrepareBulkMatchInputConfiguration> {

    private static Logger log = LoggerFactory.getLogger(PrepareBulkMatchInput.class);
    private Schema schema;

    @Autowired
    private MatchCommandService matchCommandService;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Value("${datacloud.match.max.num.blocks}")
    private Integer maxNumBlocks;

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

        avroGlobs = MatchUtils.toAvroGlobs(avroDir);
        Long count = AvroUtils.count(yarnConfiguration, avroGlobs);
        schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, avroGlobs);
        Integer[] blocks = determineBlockSizes(count);
        List<DataCloudJobConfiguration> configurations = readAndSplitInputAvro(blocks);

        executionContext.put(BulkMatchContextKey.YARN_JOB_CONFIGS, configurations);
        putStringValueInContext(BulkMatchContextKey.ROOT_OPERATION_UID, getConfiguration().getRootOperationUid());
        matchCommandService.update(getConfiguration().getRootOperationUid()) //
                .status(MatchStatus.MATCHING) //
                .rowsRequested(count.intValue()) //
                .progress(0.05f) //
                .commit();

        log.info("Execution Context=" + JsonUtils.serialize(executionContext));
    }

    private Integer[] determineBlockSizes(Long count) {
        if (MatchUtils.isValidForAccountMasterBasedMatch(getConfiguration().getMatchInput().getDataCloudVersion())) {
            if (getConfiguration().getMatchInput().isFetchOnly()) {
                return divideIntoNumBlocks(count, determineNumBlocksForAM(count));
            } else {
                return new Integer[] { count.intValue() };
            }
        } else {
            return divideIntoNumBlocks(count, determineNumBlocks(count));
        }
    }

    private Integer determineNumBlocksForAM(Long count) {
        Integer minBlockSize = 25_000;
        Integer maxBlockSize = 120_000;

        Integer numBlocks;

        if (count < (minBlockSize * maxNumBlocks)) {
            numBlocks = Math.max((int) (count / minBlockSize), 1);
        } else if (count > (maxBlockSize * maxNumBlocks)) {
            numBlocks = (int) (count / maxBlockSize);
        } else {
            numBlocks = maxNumBlocks;
        }

        return numBlocks;
    }

    private Integer determineNumBlocks(Long count) {
        Integer numBlocks = 1;
        Integer averageBlockSize = getConfiguration().getAverageBlockSize();
        while (count >= averageBlockSize * numBlocks && numBlocks < maxNumBlocks) {
            numBlocks++;
        }
        return numBlocks;
    }

    private Integer[] divideIntoNumBlocks(Long count, Integer numBlocks) {
        Long blockSize = count / numBlocks;
        Integer[] blocks = new Integer[numBlocks];
        Long sum = 0L;
        for (int i = 0; i < numBlocks - 1; i++) {
            blocks[i] = blockSize.intValue();
            sum += blockSize.intValue();
        }
        blocks[numBlocks - 1] = new Long(count - sum).intValue();
        log.info("Divide input into blocks [" + StringUtils.join(blocks, ", ") + "]");
        return blocks;
    }

    private List<DataCloudJobConfiguration> readAndSplitInputAvro(Integer[] blocks) {
        Iterator<GenericRecord> iterator = AvroUtils.iterator(yarnConfiguration, avroGlobs);
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
        String matchInputFile = FilenameUtils.getFullPath(avroPath)
                + "MatchInput_" + jobConfiguration.getRootOperationUid() + ".json";
        try {
            MatchInput matchInput = jobConfiguration.getMatchInput();
            if (matchInput.getInputBuffer() != null) {
                HdfsUtils.writeToFile(yarnConfiguration, matchInputFile, JsonUtils.serialize(matchInput));
                log.info("Write MatchInput on to hdfs, path=" + matchInputFile);
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
        int bufferSize = 2000;
        List<GenericRecord> data = new ArrayList<>();
        int count = 0;
        while (count < blockSize && iterator.hasNext()) {
            data.add(iterator.next());
            count++;
            if (data.size() >= bufferSize) {
                writeBuffer(targetFile, data);
                data.clear();
            }
        }
        if (data.size() > 0) {
            writeBuffer(targetFile, data);
        }
        log.info("Write a block of " + count + " rows to " + targetFile);
    }

    private void writeBuffer(String targetFile, List<GenericRecord> data) {
        try {
            if (!HdfsUtils.fileExists(yarnConfiguration, targetFile)) {
                AvroUtils.writeToHdfsFile(yarnConfiguration, schema, targetFile, data);
            } else {
                AvroUtils.appendToHdfsFile(yarnConfiguration, targetFile, data);
            }
            log.info("Write a buffer of " + data.size() + " rows to " + targetFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
