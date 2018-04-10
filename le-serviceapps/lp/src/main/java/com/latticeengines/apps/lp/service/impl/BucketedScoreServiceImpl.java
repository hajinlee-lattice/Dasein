package com.latticeengines.apps.lp.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.apps.core.service.ActionService;
import com.latticeengines.apps.lp.entitymgr.BucketMetadataEntityMgr;
import com.latticeengines.apps.lp.entitymgr.BucketedScoreSummaryEntityMgr;
import com.latticeengines.apps.lp.entitymgr.ModelSummaryEntityMgr;
import com.latticeengines.apps.lp.repository.writer.ModelSummaryWriterRepository;
import com.latticeengines.apps.lp.service.BucketedScoreService;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionConfiguration;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineActionConfiguration;
import com.latticeengines.domain.exposed.serviceapps.lp.CreateBucketMetadataRequest;

@Service("bucketedScoreService")
public class BucketedScoreServiceImpl implements BucketedScoreService {

    private static final Logger log = LoggerFactory.getLogger(BucketedScoreServiceImpl.class);

    @Inject
    private BucketMetadataEntityMgr bucketMetadataEntityMgr;

    @Inject
    private BucketedScoreSummaryEntityMgr bucketedScoreSummaryEntityMgr;

    @Inject
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Inject
    private ModelSummaryWriterRepository modelSummaryRepository;

    @Inject
    private ActionService actionService;

    @Override
    public Map<Long, List<BucketMetadata>> getModelBucketMetadataGroupedByCreationTimes(String modelId) {
        List<BucketMetadata> list = bucketMetadataEntityMgr.getBucketMetadatasForModelFromReader(modelId);
        return groupByCreationTime(list);
    }

    @Override
    public Map<Long, List<BucketMetadata>> getRatingEngineBucketMetadataGroupedByCreationTimes(String ratingEngineId) {
        List<BucketMetadata> list = bucketMetadataEntityMgr.getBucketMetadatasForEngineFromReader(ratingEngineId);
        return groupByCreationTime(list);
    }

    @Override
    public List<BucketMetadata> getABCDBucketsByModelGuid(String modelId) {
        return bucketMetadataEntityMgr.getUpToDateBucketMetadatasForModelFromReader(modelId);
    }

    @Override
    public List<BucketMetadata> getABCDBucketsByRatingEngineId(String ratingEngineId) {
        return bucketMetadataEntityMgr.getUpToDateBucketMetadatasForEngineFromReader(ratingEngineId);
    }

    @Override
    public void createABCDBuckets(CreateBucketMetadataRequest request) {
        if (StringUtils.isBlank(request.getModelGuid())) {
            throw new IllegalArgumentException("Must specify model GUID");
        }
        List<BucketMetadata> bucketMetadataList = request.getBucketMetadataList();
        long creationTimestamp = System.currentTimeMillis();
        bucketMetadataList.forEach(bucketMetadata -> {
            bucketMetadata.setCreationTimestamp(creationTimestamp);
            bucketMetadata.setLastModifiedByUser(request.getLastModifiedBy());
        });
        bucketMetadataEntityMgr.createBucketMetadata(bucketMetadataList, request.getModelGuid(),
                request.getRatingEngineId());
        if (StringUtils.isNotBlank(request.getModelGuid())) {
            modelSummaryEntityMgr.updateLastUpdateTime(request.getModelGuid());
        }
        if (StringUtils.isNotBlank(request.getRatingEngineId())) {
            registerAction(request.getRatingEngineId(), request.getModelGuid(), request.getLastModifiedBy());
        }
    }

    @Override
    public BucketedScoreSummary getBucketedScoreSummaryByModelGuid(String modelGuid) {
        return bucketedScoreSummaryEntityMgr.getByModelGuidFromReader(modelGuid);
    }

    @Override
    public BucketedScoreSummary createOrUpdateBucketedScoreSummary(String modelGuid,
                                                            BucketedScoreSummary bucketedScoreSummary) {
        ModelSummary modelSummary = modelSummaryRepository.findById(modelGuid);
        bucketedScoreSummary.setModelSummary(modelSummary);
        BucketedScoreSummary existing = bucketedScoreSummaryEntityMgr.getByModelGuid(modelGuid);
        if (existing != null) {
            bucketedScoreSummary.setPid(existing.getPid());
            bucketedScoreSummaryEntityMgr.update(bucketedScoreSummary);
        } else {
            bucketedScoreSummaryEntityMgr.create(bucketedScoreSummary);
        }
        return bucketedScoreSummary;
    }

    private Map<Long, List<BucketMetadata>> groupByCreationTime(List<BucketMetadata> bucketMetadatas) {
        Map<Long, List<BucketMetadata>> creationTimesToBucketMetadatas = new HashMap<>();

        for (BucketMetadata bucketMetadata : bucketMetadatas) {
            if (!creationTimesToBucketMetadatas.containsKey(bucketMetadata.getCreationTimestamp())) {
                creationTimesToBucketMetadatas.put(bucketMetadata.getCreationTimestamp(), new ArrayList<>());
            }
            creationTimesToBucketMetadatas.get(bucketMetadata.getCreationTimestamp()).add(bucketMetadata);
        }

        return creationTimesToBucketMetadatas;
    }

    private void registerAction(String ratingEngineId, String modelGuid, String userId) {
        log.info(String.format("Register AI_MODEL_BUCKET_CHANGE creation Action for RatingEngine %s, Model GUID %s",
                ratingEngineId, modelGuid));
        Action action = new Action();
        action.setType(ActionType.RATING_ENGINE_CHANGE);
        action.setActionInitiator(userId);
        ActionConfiguration actionConfiguration = new RatingEngineActionConfiguration();
        ((RatingEngineActionConfiguration) actionConfiguration)
                .setSubType(RatingEngineActionConfiguration.SubType.AI_MODEL_BUCKET_CHANGE);
        ((RatingEngineActionConfiguration) actionConfiguration).setRatingEngineId(ratingEngineId);
        ((RatingEngineActionConfiguration) actionConfiguration).setModelId(modelGuid);
        action.setActionConfiguration(actionConfiguration);
        action.setDescription(action.getActionConfiguration().serialize());
        log.debug(String.format("Registering action %s", action));
        actionService.create(action);
    }

}
