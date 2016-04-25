package com.latticeengines.propdata.workflow.match.steps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.propdata.PropDataJobConfiguration;
import com.latticeengines.domain.exposed.propdata.match.MatchStatus;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.service.impl.HdfsPodContext;
import com.latticeengines.propdata.match.service.MatchCommandService;
import com.latticeengines.propdata.match.util.MatchUtils;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("prepareBulkMatchInput")
@Scope("prototype")
public class PrepareBulkMatchInput extends BaseWorkflowStep<PrepareBulkMatchInputConfiguration> {

    private static Log log = LogFactory.getLog(PrepareBulkMatchInput.class);
    private Schema schema;

    @Autowired
    private MatchCommandService matchCommandService;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Value("${propdata.match.max.num.blocks:4}")
    private Integer maxNumBlocks;

    @Value("${propdata.match.num.threads:4}")
    private Integer threadPoolSize;

    @Value("${propdata.match.group.size:20}")
    private Integer groupSize;

    private String avroGlobs;

    @Override
    public void execute() {
        log.info("Inside PrepareBulkMatchInput execute()");
        String avroDir = getConfiguration().getInputAvroDir();
        HdfsPodContext.changeHdfsPodId(getConfiguration().getHdfsPodId());

        avroGlobs = MatchUtils.toAvroGlobs(avroDir);
        Long count = AvroUtils.count(yarnConfiguration, avroGlobs);
        schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, avroGlobs);
        Integer[] blocks = determineBlockSizes(count);
        List<PropDataJobConfiguration> configurations = readAndSplitInputAvro(blocks);
        executionContext.put(BulkMatchContextKey.YARN_JOB_CONFIGS, configurations);
        executionContext.put(BulkMatchContextKey.ROOT_OPERATION_UID, getConfiguration().getRootOperationUid());
        matchCommandService.update(getConfiguration().getRootOperationUid()) //
                .status(MatchStatus.MATCHING) //
                .progress(0.05f) //
                .commit();
    }

    private Integer[] determineBlockSizes(Long count) {
        return divideIntoNumBlocks(count, determineNumBlocks(count));
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

    private List<PropDataJobConfiguration> readAndSplitInputAvro(Integer[] blocks) {
        Iterator<GenericRecord> iterator = AvroUtils.iterator(yarnConfiguration, avroGlobs);
        List<PropDataJobConfiguration> configurations = new ArrayList<>();

        int blockIdx = 0;
        for (Integer blockSize : blocks) {
            blockIdx++;
            String blockOperationUid = UUID.randomUUID().toString().toUpperCase();
            if (blocks.length == 1) {
                blockOperationUid = getConfiguration().getRootOperationUid();
            }

            PropDataJobConfiguration jobConfiguration = generateJobConfiguration();
            jobConfiguration.setBlockSize(blockSize);
            jobConfiguration.setBlockOperationUid(blockOperationUid);
            String targetFile = hdfsPathBuilder.constructMatchBlockInputAvro(jobConfiguration.getRootOperationUid(),
                    jobConfiguration.getBlockOperationUid()).toString();
            jobConfiguration.setAvroPath(targetFile);
            jobConfiguration.setAppName(String.format("PropDataMatch[%s]~Block(%d/%d)[%s]~%s",
                    getConfiguration().getRootOperationUid(), blockIdx, blocks.length, blockOperationUid,
                    getConfiguration().getCustomerSpace().toString()));
            configurations.add(jobConfiguration);

            List<GenericRecord> data = new ArrayList<>();
            while (data.size() < blockSize && iterator.hasNext()) {
                data.add(iterator.next());
            }
            try {
                AvroUtils.writeToHdfsFile(yarnConfiguration, schema, targetFile, data);
                log.info("Write a block of " + AvroUtils.count(yarnConfiguration, targetFile) + " rows to "
                        + targetFile);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return configurations;
    }

    private PropDataJobConfiguration generateJobConfiguration() {
        PropDataJobConfiguration jobConfiguration = new PropDataJobConfiguration();
        jobConfiguration.setHdfsPodId(getConfiguration().getHdfsPodId());
        jobConfiguration.setReturnUnmatched(getConfiguration().getReturnUnmatched());
        jobConfiguration.setName("PropDataMatchBlock");
        jobConfiguration.setCustomerSpace(getConfiguration().getCustomerSpace());
        jobConfiguration.setPredefinedSelection(getConfiguration().getPredefinedSelection());
        jobConfiguration.setKeyMap(getConfiguration().getKeyMap());
        jobConfiguration.setRootOperationUid(getConfiguration().getRootOperationUid());
        jobConfiguration.setGroupSize(groupSize);
        jobConfiguration.setThreadPoolSize(threadPoolSize);
        jobConfiguration.setSingleBlock(false);
        return jobConfiguration;
    }

}
