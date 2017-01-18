package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
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
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.pls.entitymanager.BucketMetadataEntityMgr;
import com.latticeengines.pls.service.BucketedScoreService;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.pls.service.WorkflowJobService;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("bucketedScoreService")
public class BucketedScoreServiceImpl implements BucketedScoreService {

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

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

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
        int currentRecord = pivotedRecords.size() - 1;

        for (int i = 99; i > 4; i--) {
            GenericRecord pivotedRecord = pivotedRecords.get(currentRecord);
            if ((int) pivotedRecord.get(SCORE) == i) {
                bucketedScoreSummary.getBucketedScores()[i] = new BucketedScore((int) pivotedRecord.get(SCORE),
                        new Long((long) pivotedRecord.get(TOTAL_EVENTS)).intValue(),
                        new Double((double) pivotedRecord.get(TOTAL_POSITIVE_EVENTS)).intValue(), cumulativeNumLeads,
                        cumulativeNumConverted);
                cumulativeNumLeads += new Long((long) pivotedRecord.get(TOTAL_EVENTS)).intValue();
                cumulativeNumConverted += new Double((double) pivotedRecord.get(TOTAL_POSITIVE_EVENTS)).intValue();
                currentRecord--;
            } else {
                bucketedScoreSummary.getBucketedScores()[i] = new BucketedScore(i, 0, 0, cumulativeNumLeads,
                        cumulativeNumConverted);
            }
        }
        bucketedScoreSummary.getBucketedScores()[4] = new BucketedScore(4, 0, 0, cumulativeNumLeads,
                cumulativeNumConverted);
        bucketedScoreSummary.setTotalNumLeads(cumulativeNumLeads);
        bucketedScoreSummary.setTotalNumConverted(cumulativeNumConverted);

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
                .findBucketMetadatasForModelId(Long.toString(modelSummary.getPid()));
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
                .findUpToDateBucketMetadatasForModelId(Long.toString(modelSummary.getPid()));

        return bucketMetadatas;

    }

    @Override
    public void createBucketMetadatas(String modelId, List<BucketMetadata> bucketMetadatas) {
        ModelSummary modelSummary = modelSummaryService.getModelSummaryByModelId(modelId);
        Long creationTimestamp = System.currentTimeMillis();
        Tenant tenant = tenantEntityMgr.findByTenantId(MultiTenantContext.getTenant().getId());

        for (BucketMetadata bucketMetadata : bucketMetadatas) {
            bucketMetadata.setCreationTimestamp(creationTimestamp);
            bucketMetadata.setModelSummary(modelSummary);
            bucketMetadata.setTenant(tenant);
            bucketMetadataEntityMgr.create(bucketMetadata);
        }
    }

}
