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
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionConfiguration;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineActionConfiguration;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.BucketedScoreSummaryUtils;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.pls.entitymanager.BucketMetadataEntityMgr;
import com.latticeengines.pls.entitymanager.BucketedScoreSummaryEntityMgr;
import com.latticeengines.pls.service.ActionService;
import com.latticeengines.pls.service.BucketedScoreService;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.pls.service.WorkflowJobService;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;

@Component("bucketedScoreService")
public class BucketedScoreServiceImpl implements BucketedScoreService {

    private static final Logger log = LoggerFactory.getLogger(BucketedScoreServiceImpl.class);

    @Inject
    private ModelSummaryService modelSummaryService;

    @Inject
    private ActionService actionService;

    @Inject
    private WorkflowJobService workflowJobService;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private BucketMetadataEntityMgr bucketMetadataEntityMgr;

    @Inject
    private BucketedScoreSummaryEntityMgr bucketedScoreSummaryEntityMgr;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Override
    public BucketedScoreSummary getBucketedScoreSummaryForModelId(String modelId) throws Exception {
        ModelSummary modelSummary = modelSummaryService.findByModelId(modelId, false, false, false);
        BucketedScoreSummary bucketedScoreSummary = bucketedScoreSummaryEntityMgr.findByModelSummary(modelSummary);
        if (bucketedScoreSummary != null) {
            return bucketedScoreSummary;
        }
        return getBucketedScoreSummaryBasedOnModelSummary(modelSummary);
    }

    @Override
    public BucketedScoreSummary createBucketedScoreSummaryForModelId(String modelId,
            BucketedScoreSummary bucketedScoreSummary) throws Exception {
        ModelSummary modelSummary = modelSummaryService.findByModelId(modelId, false, false, false);
        createBucketedScoreSummary(modelSummary, bucketedScoreSummary);
        return bucketedScoreSummary;
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

        BucketedScoreSummary bucketedScoreSummary = BucketedScoreSummaryUtils
                .generateBucketedScoreSummary(pivotedRecords);
        createBucketedScoreSummary(modelSummary, bucketedScoreSummary);
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

    private void createBucketedScoreSummary(ModelSummary modelSummary, BucketedScoreSummary bucketedScoreSummary) {
        bucketedScoreSummary.setModelSummary(modelSummary);
        bucketedScoreSummaryEntityMgr.create(bucketedScoreSummary);
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
        // Register RatingEngineConfiguration.SubType.AI_MODEL_BUCKET_CHANGE
        // Action
        log.info(String.format("Register AI_MODEL_BUCKET_CHANGE creation Action for RatingEngine %s, Rating Model %s",
                ratingEngineId, modelId));
        registerAction(ratingEngineId, modelId);

        // Update the status of the Rating Engine to be active by default
        activateRatingEngine(ReAndMs.getLeft());
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
        AIModel activeModel = (AIModel) ratingEngine.getActiveModel();
        ModelSummary modelSummary = activeModel.getModelSummary();
        if (modelSummary != null) {
            return bucketMetadataEntityMgr.getUpToDateBucketMetadatasForModelId(Long.toString(modelSummary.getPid()));
        } else {
            throw new LedpException(LedpCode.LEDP_40020,
                    new String[] { activeModel.getId(), ratingEngineId, tenant.getId() });
        }
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

    private void registerAction(String ratingEngineId, String ratingModelId) {
        Action action = new Action();
        action.setTenant(MultiTenantContext.getTenant());
        action.setType(ActionType.RATING_ENGINE_CHANGE);
        action.setActionInitiator(MultiTenantContext.getEmailAddress());
        ActionConfiguration actionConfiguration = new RatingEngineActionConfiguration();
        ((RatingEngineActionConfiguration) actionConfiguration)
                .setSubType(RatingEngineActionConfiguration.SubType.AI_MODEL_BUCKET_CHANGE);
        ((RatingEngineActionConfiguration) actionConfiguration).setRatingEngineId(ratingEngineId);
        ((RatingEngineActionConfiguration) actionConfiguration).setModelId(ratingModelId);
        action.setActionConfiguration(actionConfiguration);
        action.setDescription(action.getActionConfiguration().serialize());
        log.debug(String.format("Registering action %s", action));
        actionService.create(action);
    }

    private void activateRatingEngine(RatingEngine ratingEngine) {
        if (ratingEngine.getStatus() == RatingEngineStatus.INACTIVE) {
            ratingEngine.setStatus(RatingEngineStatus.ACTIVE);
            ratingEngineProxy.createOrUpdateRatingEngine(MultiTenantContext.getCustomerSpace().toString(),
                    ratingEngine);
        }
    }

}
