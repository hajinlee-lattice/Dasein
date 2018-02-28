package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketedScore;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.pls.entitymanager.BucketMetadataEntityMgr;
import com.latticeengines.pls.service.BucketedScoreService;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.pls.service.WorkflowJobService;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;

@Component("bucketedScoreService")
public class BucketedScoreServiceImpl implements BucketedScoreService {

    private static final Logger log = LoggerFactory.getLogger(BucketedScoreServiceImpl.class);
    private static final String SCORE = "Score";
    private static final String TOTAL_EVENTS = "TotalEvents";
    private static final String TOTAL_POSITIVE_EVENTS = "TotalPositiveEvents";

    @Inject
    private ModelSummaryService modelSummaryService;

    @Inject
    private WorkflowJobService workflowJobService;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private BucketMetadataEntityMgr bucketMetadataEntityMgr;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Override
    public BucketedScoreSummary getBucketedScoreSummaryForModelId(String modelId) throws Exception {
        ModelSummary modelSummary = modelSummaryService.findByModelId(modelId, false, false, false);
        return getBucketedScoreSummaryBasedOnModelSummary(modelSummary);
    }

    private BucketedScoreSummary getBucketedScoreSummaryBasedOnModelSummary(ModelSummary modelSummary)
            throws Exception {
        BucketedScoreSummary bucketedScoreSummary = new BucketedScoreSummary();
        String jobId = modelSummary.getModelSummaryConfiguration().getString(ProvenancePropertyName.WorkflowJobId);
        String pivotAvroDirPath = null;

        if (jobId == null || workflowJobService.find(jobId) == null) {
            throw new LedpException(LedpCode.LEDP_18125, new String[] { modelSummary.getId() });
        } else {
            Job job = workflowJobService.find(jobId);
            pivotAvroDirPath = job.getOutputs().get(WorkflowContextConstants.Outputs.PIVOT_SCORE_AVRO_PATH);
        }

        log.info(String.format("Looking for pivoted score avro for model: %s at path: %s", modelSummary.getId(),
                pivotAvroDirPath));
        if (pivotAvroDirPath == null) {
            throw new LedpException(LedpCode.LEDP_18125, new String[] { modelSummary.getId() });
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
        ModelSummary modelSummary = modelSummaryService.findByModelId(modelId, false, false, true);
        if (modelSummary == null) {
            throw new LedpException(LedpCode.LEDP_18126, new String[] { modelId });
        }

        List<BucketMetadata> bucketMetadatas = bucketMetadataEntityMgr
                .getBucketMetadatasForModelId(Long.toString(modelSummary.getPid()));
        return groupByCreationTime(bucketMetadatas);
    }

    @Override
    public List<BucketMetadata> getUpToDateModelBucketMetadata(String modelId) {
        ModelSummary modelSummary = modelSummaryService.findByModelId(modelId, false, false, false);
        return getBucketMetadataListBasedModelSummary(modelSummary, modelId);
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

    @Override
    public List<BucketMetadata> getUpToDateModelBucketMetadataAcrossTenants(String modelId) {
        ModelSummary modelSummary = modelSummaryService.getModelSummaryByModelId(modelId);
        return getBucketMetadataListBasedModelSummary(modelSummary, modelId);
    }

    private List<BucketMetadata> getBucketMetadataListBasedModelSummary(ModelSummary modelSummary, String modelId) {
        if (modelSummary == null) {
            throw new LedpException(LedpCode.LEDP_18126, new String[] { modelId });
        }

        List<BucketMetadata> bucketMetadatas = bucketMetadataEntityMgr
                .getUpToDateBucketMetadatasForModelId(Long.toString(modelSummary.getPid()));
        return bucketMetadatas;
    }

    @Override
    public void createBucketMetadatas(String ratingEngineId, String modelId, List<BucketMetadata> bucketMetadatas,
            String userId) {
        log.info(
                String.format("Creating BucketMetadata for RatingEngine %s, Rating Model %s", ratingEngineId, modelId));
        ImmutablePair<RatingEngine, ModelSummary> ReAndMs = getModelSummaryAndRatingEngine(ratingEngineId, modelId);
        Long creationTimestamp = System.currentTimeMillis();
        modelSummaryService.updateLastUpdateTime(ReAndMs.getRight().getId());
        for (BucketMetadata bucketMetadata : bucketMetadatas) {
            bucketMetadata.setLastModifiedByUser(userId);
            bucketMetadata.setCreationTimestamp(creationTimestamp);
            bucketMetadata.setModelSummary(ReAndMs.getRight());
            bucketMetadata.setRatingEngine(ReAndMs.getLeft());
            bucketMetadataEntityMgr.create(bucketMetadata);
        }

    }

    @Override
    public List<BucketMetadata> getUpToDateABCDBucketsBasedOnRatingEngineId(String ratingEngineId) {
        Tenant tenant = MultiTenantContext.getTenant();
        log.info(String.format("Get udpate-to-date BucketMetadata for RatingEngine %s, Tenant %s", ratingEngineId,
                tenant.getId()));
        RatingEngine ratingEngine = ratingEngineProxy.getRatingEngine(tenant.getId(), ratingEngineId);
        if (ratingEngine == null) {
            throw new NullPointerException(String.format("Cannot find Rating Engine with Id %s", ratingEngineId));
        }
        return bucketMetadataEntityMgr.getUpToDateBucketMetadatasBasedOnRatingEngine(ratingEngine);
    }

    @Override
    public Map<Long, List<BucketMetadata>> getModelBucketMetadataGroupedByCreationTimesBasedOnRatingEngineId(
            String ratingEngineId) {
        Tenant tenant = MultiTenantContext.getTenant();
        log.info(String.format("Get BucketMetadata grouped by Creation Time for RatingEngine %s, Tenant %s",
                ratingEngineId, tenant.getId()));
        RatingEngine ratingEngine = ratingEngineProxy.getRatingEngine(tenant.getId(), ratingEngineId);
        if (ratingEngine == null) {
            throw new NullPointerException(String.format("Cannot find Rating Engine with Id %s", ratingEngineId));
        }

        List<BucketMetadata> bucketMetadatas = bucketMetadataEntityMgr
                .getBucketMetadatasBasedOnRatingEngine(ratingEngine);
        return groupByCreationTime(bucketMetadatas);
    }

    private Map<Long, List<BucketMetadata>> groupByCreationTime(List<BucketMetadata> bucketMetadatas) {
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
    public BucketedScoreSummary getBuckedScoresSummaryBasedOnRatingEngineAndRatingModel(String ratingEngineId,
            String modelId) throws Exception {
        log.info(String.format("Get BuckedScoresSummary given RatingEngineId %s and ModelId %s", ratingEngineId,
                modelId));
        ImmutablePair<RatingEngine, ModelSummary> ReAndMs = getModelSummaryAndRatingEngine(ratingEngineId, modelId);
        ModelSummary modelSummary = ReAndMs.getRight();
        return getBucketedScoreSummaryBasedOnModelSummary(modelSummary);
    }

    private ImmutablePair<RatingEngine, ModelSummary> getModelSummaryAndRatingEngine(String ratingEngineId,
            String modelId) {
        Tenant tenant = MultiTenantContext.getTenant();
        RatingEngine ratingEngine = ratingEngineProxy.getRatingEngine(tenant.getId(), ratingEngineId);
        if (ratingEngine == null) {
            throw new NullPointerException(String.format("Cannot find Rating Engine with Id %s", ratingEngineId));
        }
        RatingModel ratingModel = ratingEngineProxy.getRatingModel(tenant.getId(), ratingEngineId, modelId);
        ModelSummary modelSummary = null;
        if (ratingModel != null && ratingModel instanceof AIModel) {
            AIModel aiModel = (AIModel) ratingModel;
            modelSummary = aiModel.getModelSummary();
        } else {
            throw new LedpException(LedpCode.LEDP_18179, new String[] { modelId });
        }
        modelSummary = modelSummaryService.getModelSummaryByModelId(modelSummary.getId());
        return new ImmutablePair<>(ratingEngine, modelSummary);
    }

}
