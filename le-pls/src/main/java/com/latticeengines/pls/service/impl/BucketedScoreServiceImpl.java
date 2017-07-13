package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketedScore;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.pls.entitymanager.BucketMetadataEntityMgr;
import com.latticeengines.pls.service.BucketedScoreService;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.pls.service.WorkflowJobService;

@Component("bucketedScoreService")
public class BucketedScoreServiceImpl implements BucketedScoreService {

    private static final Logger log = LoggerFactory.getLogger(BucketedScoreServiceImpl.class);
    private static final String SCORE = "Score";
    private static final String TOTAL_EVENTS = "TotalEvents";
    private static final String TOTAL_POSITIVE_EVENTS = "TotalPositiveEvents";

    @Autowired
    private ModelSummaryService modelSummaryService;

    @Autowired
    private WorkflowJobService workflowJobService;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private BucketMetadataEntityMgr bucketMetadataEntityMgr;

    @Override
    public BucketedScoreSummary getBucketedScoreSummaryForModelId(String modelId) throws Exception {
        BucketedScoreSummary bucketedScoreSummary = new BucketedScoreSummary();
        ModelSummary modelSummary = modelSummaryService.getModelSummaryByModelId(modelId);

        String jobId = modelSummary.getModelSummaryConfiguration().getString(ProvenancePropertyName.WorkflowJobId);
        String pivotAvroDirPath = null;

        if (jobId == null || workflowJobService.find(jobId) == null) {
            throw new LedpException(LedpCode.LEDP_18125, new String[] { modelId });
        } else {
            Job job = workflowJobService.find(jobId);
            pivotAvroDirPath = job.getOutputs().get(WorkflowContextConstants.Outputs.PIVOT_SCORE_AVRO_PATH);
        }

        log.info(String.format("Looking for pivoted score avro for model: %s at path: %s", modelId, pivotAvroDirPath));
        if (pivotAvroDirPath == null) {
            throw new LedpException(LedpCode.LEDP_18125, new String[] { modelId });
        }
        HdfsUtils.HdfsFileFilter hdfsFileFilter = new HdfsUtils.HdfsFileFilter() {
            @Override
            public boolean accept(FileStatus file) {
                if (file == null) {
                    return false;
                }
                String name = file.getPath().getName();
                return name.matches(".*.avro");
            }
        };
        List<String> filePaths = HdfsUtils.getFilesForDir(yarnConfiguration, pivotAvroDirPath, hdfsFileFilter);
        String pivotAvroFilePath = filePaths.get(0);
        List<GenericRecord> pivotedRecords = AvroUtils.getData(yarnConfiguration, new Path(pivotAvroFilePath));

        int cumulativeNumLeads = 0, cumulativeNumConverted = 0;
        int currentRecord = pivotedRecords.size() - 1, currentScore = 99;

        while (currentScore >= 5) {
            if (currentRecord < 0) {
                break;
            }
            GenericRecord pivotedRecord = pivotedRecords.get(currentRecord);
            log.info(String.format("current record index: %s, i is: %s, and generic record is: %s", currentRecord,
                    currentScore, pivotedRecord.toString()));
            if (pivotedRecord != null
                    && Double.valueOf(pivotedRecord.get(SCORE).toString()).intValue() == currentScore) {
                bucketedScoreSummary.getBucketedScores()[currentScore] = new BucketedScore(
                        Double.valueOf(pivotedRecord.get(SCORE).toString()).intValue(),
                        Double.valueOf(pivotedRecord.get(TOTAL_EVENTS).toString()).intValue(),
                        Double.valueOf(pivotedRecord.get(TOTAL_POSITIVE_EVENTS).toString()).intValue(),
                        cumulativeNumLeads, cumulativeNumConverted);
                cumulativeNumLeads += new Long((long) pivotedRecord.get(TOTAL_EVENTS)).intValue();
                cumulativeNumConverted += new Double((double) pivotedRecord.get(TOTAL_POSITIVE_EVENTS)).intValue();
                currentRecord--;
            } else {
                bucketedScoreSummary.getBucketedScores()[currentScore] = new BucketedScore(currentScore, 0, 0,
                        cumulativeNumLeads, cumulativeNumConverted);
            }
            currentScore--;
        }
        for (; currentScore > 3; currentScore--) {
            bucketedScoreSummary.getBucketedScores()[currentScore] = new BucketedScore(currentScore, 0, 0,
                    cumulativeNumLeads, cumulativeNumConverted);
        }
        bucketedScoreSummary.setTotalNumLeads(cumulativeNumLeads);
        bucketedScoreSummary.setTotalNumConverted(cumulativeNumConverted);
        bucketedScoreSummary.setOverallLift((double) cumulativeNumConverted / cumulativeNumLeads);

        double totalLift = (double) cumulativeNumConverted / cumulativeNumLeads;
        for (int i = 32; i > 0; i--) {
            BucketedScore[] bucketedScores = bucketedScoreSummary.getBucketedScores();
            int totalLeadsInBar = bucketedScores[i * 3 + 1].getNumLeads() + bucketedScores[i * 3 + 2].getNumLeads()
                    + bucketedScores[i * 3 + 3].getNumLeads();
            int totalLeadsConvertedInBar = bucketedScores[i * 3 + 1].getNumConverted()
                    + bucketedScores[i * 3 + 2].getNumConverted() + bucketedScores[i * 3 + 3].getNumConverted();
            if (totalLeadsInBar == 0) {
                bucketedScoreSummary.getBarLifts()[32 - i] = 0;
            } else {
                bucketedScoreSummary.getBarLifts()[32 - i] = ((double) totalLeadsConvertedInBar / totalLeadsInBar)
                        / totalLift;
            }
        }

        return bucketedScoreSummary;
    }

    @Override
    public Map<Long, List<BucketMetadata>> getModelBucketMetadataGroupedByCreationTimes(String modelId) {
        ModelSummary modelSummary = modelSummaryService.getModelSummaryByModelId(modelId);
        if (modelSummary == null) {
            throw new LedpException(LedpCode.LEDP_18126, new String[] { modelId });
        }

        List<BucketMetadata> bucketMetadatas = bucketMetadataEntityMgr
                .getBucketMetadatasForModelId(Long.toString(modelSummary.getPid()));
        Map<Long, List<BucketMetadata>> creationTimesToBucketMetadatas = new HashMap<>();

        for (BucketMetadata bucketMetadata : bucketMetadatas) {
            if (!creationTimesToBucketMetadatas.containsKey(bucketMetadata.getCreationTimestamp())) {
                creationTimesToBucketMetadatas.put(bucketMetadata.getCreationTimestamp(),
                        new ArrayList<BucketMetadata>());
            }
            creationTimesToBucketMetadatas.get(bucketMetadata.getCreationTimestamp()).add(bucketMetadata);
        }

        return creationTimesToBucketMetadatas;
    }

    @Override
    public List<BucketMetadata> getUpToDateModelBucketMetadata(String modelId) {
        ModelSummary modelSummary = modelSummaryService.getModelSummaryByModelId(modelId);
        if (modelSummary == null) {
            throw new LedpException(LedpCode.LEDP_18126, new String[] { modelId });
        }

        List<BucketMetadata> bucketMetadatas = bucketMetadataEntityMgr
                .getUpToDateBucketMetadatasForModelId(Long.toString(modelSummary.getPid()));
        return bucketMetadatas;
    }

    @Override
    public void createBucketMetadatas(String modelId, List<BucketMetadata> bucketMetadatas) {
        ModelSummary modelSummary = modelSummaryService.getModelSummaryByModelId(modelId);
        Long creationTimestamp = System.currentTimeMillis();
        // update modelSummary
        modelSummaryService.updateLastUpdateTime(modelId);
        for (BucketMetadata bucketMetadata : bucketMetadatas) {
            bucketMetadata.setCreationTimestamp(creationTimestamp);
            bucketMetadata.setModelSummary(modelSummary);
            bucketMetadataEntityMgr.create(bucketMetadata);
        }
    }

}
