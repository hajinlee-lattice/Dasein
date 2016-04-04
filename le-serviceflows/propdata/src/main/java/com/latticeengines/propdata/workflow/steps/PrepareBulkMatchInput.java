package com.latticeengines.propdata.workflow.steps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.propdata.PropDataJobConfiguration;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("prepareBulkMatchInput")
@Scope("prototype")
public class PrepareBulkMatchInput extends BaseWorkflowStep<PrepareBulkMatchInputConfiguration> {

    private static Log log = LogFactory.getLog(PrepareBulkMatchInput.class);
    private Schema schema;

    @Override
    public void execute() {
        log.info("Inside PrepareBulkMatchInput execute()");
        String avroDir = getConfiguration().getInputAvroDir();
        Long count = AvroUtils.count(yarnConfiguration, avroDir + "/*.avro");
        schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, avroDir + "/*.avro");
        Integer[] blocks = determineBlockSizes(count);
        List<PropDataJobConfiguration> configurations = readAndSplitInputAvro(blocks);
        executionContext.put(BulkMatchContextKey.YARN_JOB_CONFIGS, configurations);
    }

    private Integer[] determineBlockSizes(Long count) {
        return divideIntoNumBlocks(count, determineNumBlocks(count));
    }

    private Integer determineNumBlocks(Long count) {
        Long[] thresholds = new Long[] { 3000L, 6000L, 9000L, 12000L, 15000L, 18000L, 21000L };
        for (int i = 0; i < thresholds.length; i++) {
            if (count < thresholds[i]) {
                return i + 1;
            }
        }
        return thresholds.length + 1;
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
        return blocks;
    }

    private List<PropDataJobConfiguration> readAndSplitInputAvro(Integer[] blocks) {
        Iterator<GenericRecord> iterator = AvroUtils.iterator(yarnConfiguration,
                getConfiguration().getInputAvroDir() + "/*.avro");
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
            String targetDir = getConfiguration().getTargetDir();
            String targetFile = new Path(targetDir).append(blockOperationUid.replace("-", "_").toLowerCase()).toString()
                    + ".avro";
            jobConfiguration.setAvroPath(targetFile);
            jobConfiguration.setAppName(String.format("PropDataMatch[%s]~Block(%d/%d)[%s]",
                    getConfiguration().getRootOperationUid(), blockIdx, blocks.length, blockOperationUid));
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
        jobConfiguration.setName("PropDataMatchBlock");
        jobConfiguration.setCustomerSpace(getConfiguration().getCustomerSpace());
        jobConfiguration.setPredefinedSelection(getConfiguration().getPredefinedSelection());
        jobConfiguration.setKeyMap(getConfiguration().getKeyMap());
        jobConfiguration.setRootOperationUid(getConfiguration().getRootOperationUid());
        jobConfiguration.setGroupSize(getConfiguration().getGroupSize());
        return jobConfiguration;
    }

}
