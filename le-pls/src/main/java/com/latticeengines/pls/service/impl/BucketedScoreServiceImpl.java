package com.latticeengines.pls.service.impl;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.conf.Configuration;
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
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.lp.CreateBucketMetadataRequest;
import com.latticeengines.domain.exposed.util.BucketedScoreSummaryUtils;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.pls.service.BucketedScoreService;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.pls.service.WorkflowJobService;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.lp.BucketedScoreProxy;

@Component("bucketedScoreService")
public class BucketedScoreServiceImpl implements BucketedScoreService {

    private static final Logger log = LoggerFactory.getLogger(BucketedScoreServiceImpl.class);

    @Inject
    private ModelSummaryService modelSummaryService;

    @Inject
    private WorkflowJobService workflowJobService;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Inject
    private BucketedScoreProxy bucketedScoreProxy;

    @Override
    public BucketedScoreSummary getBucketedScoreSummaryForModelId(String modelId) throws Exception {
        BucketedScoreSummary bucketedScoreSummary = bucketedScoreProxy.getBucketedScoreSummary(modelId);
        if (bucketedScoreSummary == null) {
            ModelSummary modelSummary = modelSummaryService.findByModelId(modelId, false, false, false);
            bucketedScoreSummary = getBucketedScoreSummaryBasedOnModelSummary(modelSummary);
        }
        return bucketedScoreSummary;
    }

    @Override
    public BucketedScoreSummary createOrUpdateBucketedScoreSummary(String modelId,
            BucketedScoreSummary bucketedScoreSummary) throws Exception {
        return bucketedScoreProxy.createOrUpdateBucketedScoreSummary(modelId, bucketedScoreSummary);
    }

    private BucketedScoreSummary getBucketedScoreSummaryBasedOnModelSummary(ModelSummary modelSummary)
            throws Exception {
        String jobId = modelSummary.getModelSummaryConfiguration().getString(ProvenancePropertyName.WorkflowJobId);
        String pivotAvroDirPath = null;

        if (jobId == null || workflowJobService.find(jobId, false) == null) {
            throw new LedpException(LedpCode.LEDP_18125, new String[] { modelSummary.getId() });
        } else {
            Job job = workflowJobService.find(jobId, false);
            pivotAvroDirPath = job.getOutputs().get(WorkflowContextConstants.Outputs.PIVOT_SCORE_AVRO_PATH);
        }

        log.info(String.format("Looking for pivoted score avro for model: %s at path: %s", modelSummary.getId(),
                pivotAvroDirPath));
        if (pivotAvroDirPath == null) {
            throw new LedpException(LedpCode.LEDP_18125, new String[] { modelSummary.getId() });
        }

        List<String> filePaths = HdfsUtils.getFilesForDir(yarnConfiguration, pivotAvroDirPath, ".*.avro");
        String pivotAvroFilePath = filePaths.get(0);
        List<GenericRecord> pivotedRecords = AvroUtils.getData(yarnConfiguration, new Path(pivotAvroFilePath));

        BucketedScoreSummary bucketedScoreSummary = BucketedScoreSummaryUtils
                .generateBucketedScoreSummary(pivotedRecords);
        bucketedScoreProxy.createOrUpdateBucketedScoreSummary(modelSummary.getId(), bucketedScoreSummary);
        log.info("Copy bucketed score summary from avro to db for model " + modelSummary.getId());
        return bucketedScoreSummary;
    }

    @Override
    public Map<Long, List<BucketMetadata>> getModelBucketMetadataGroupedByCreationTimes(String modelId) {
        return bucketedScoreProxy.getABCDBucketsByModelGuid(modelId);
    }

    @Override
    public List<BucketMetadata> getUpToDateModelBucketMetadata(String modelId) {
        return bucketedScoreProxy.getLatestABCDBucketsByModelGuid(modelId);
    }

    @Override
    public void createBucketMetadatas(String modelId, List<BucketMetadata> bucketMetadatas) {
        CreateBucketMetadataRequest request = new CreateBucketMetadataRequest();
        request.setBucketMetadataList(bucketMetadatas);
        request.setLastModifiedBy(MultiTenantContext.getEmailAddress());
        request.setModelGuid(modelId);
        request.setTenantId(MultiTenantContext.getTenantId());
        bucketedScoreProxy.createABCDBuckets(request);
    }

    @Override
    public List<BucketMetadata> getUpToDateModelBucketMetadataAcrossTenants(String modelId) {
        return bucketedScoreProxy.getLatestABCDBucketsByModelGuid(modelId);
    }

    @Override
    public void createBucketMetadatas(String ratingEngineId, String modelId, List<BucketMetadata> bucketMetadatas,
            String userId) {
        log.info(
                String.format("Creating BucketMetadata for RatingEngine %s, Model %s", ratingEngineId, modelId));
        CreateBucketMetadataRequest request = new CreateBucketMetadataRequest();
        request.setBucketMetadataList(bucketMetadatas);
        request.setModelGuid(modelId);
        request.setRatingEngineId(ratingEngineId);
        request.setLastModifiedBy(userId);
        request.setTenantId(MultiTenantContext.getTenantId());
        bucketedScoreProxy.createABCDBuckets(request);
    }

    @Override
    public List<BucketMetadata> getUpToDateABCDBucketsBasedOnRatingEngineId(String ratingEngineId) {
        return bucketedScoreProxy.getLatestABCDBucketsByEngineId(ratingEngineId);
    }

    @Override
    public Map<Long, List<BucketMetadata>> getModelBucketMetadataGroupedByCreationTimesBasedOnRatingEngineId(
            String ratingEngineId) {
        return bucketedScoreProxy.getABCDBucketsByEngineId(ratingEngineId);
    }

    @Override
    public BucketedScoreSummary getBuckedScoresSummaryBasedOnRatingEngineAndRatingModel(String ratingEngineId,
            String modelId) throws Exception {
        log.info(String.format("Get BuckedScoresSummary given RatingEngineId %s and ModelId %s", ratingEngineId,
                modelId));
        ImmutablePair<RatingEngine, ModelSummary> ReAndMs = getModelSummaryAndRatingEngine(ratingEngineId, modelId);
        ModelSummary modelSummary = ReAndMs.getRight();
        return getBucketedScoreSummaryForModelId(modelSummary.getId());
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
