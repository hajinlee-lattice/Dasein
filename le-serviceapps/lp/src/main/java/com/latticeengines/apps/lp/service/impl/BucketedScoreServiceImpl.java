package com.latticeengines.apps.lp.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.apps.core.service.ActionService;
import com.latticeengines.apps.lp.entitymgr.BucketMetadataEntityMgr;
import com.latticeengines.apps.lp.entitymgr.BucketedScoreSummaryEntityMgr;
import com.latticeengines.apps.lp.repository.writer.ModelSummaryWriterRepository;
import com.latticeengines.apps.lp.service.BucketedScoreService;
import com.latticeengines.apps.lp.service.ModelSummaryService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineActionConfiguration;
import com.latticeengines.domain.exposed.serviceapps.lp.CreateBucketMetadataRequest;
import com.latticeengines.domain.exposed.serviceapps.lp.UpdateBucketMetadataRequest;
import com.latticeengines.domain.exposed.util.BucketedScoreSummaryUtils;

@Service("bucketedScoreService")
public class BucketedScoreServiceImpl implements BucketedScoreService {

    private static final Logger log = LoggerFactory.getLogger(BucketedScoreServiceImpl.class);

    @Inject
    private BucketMetadataEntityMgr bucketMetadataEntityMgr;

    @Inject
    private BucketedScoreSummaryEntityMgr bucketedScoreSummaryEntityMgr;

    @Inject
    private ModelSummaryWriterRepository modelSummaryRepository;

    @Inject
    private ActionService actionService;

    @Inject
    private ModelSummaryService modelSummaryService;

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
    public List<BucketMetadata> getModelABCDBucketsByModelGuid(String modelId) {
        return bucketMetadataEntityMgr.getModelBucketMetadatasFromReader(modelId);
    }
    
    @Override
    public List<BucketMetadata> getPublishedBucketMetadataByModelGuid(String modelSummaryId) {
        Integer maxPublishedVersion = bucketMetadataEntityMgr.getMaxPublishedVersionByModelId(modelSummaryId);
        if (maxPublishedVersion == null) {
            return new ArrayList<>();
        }
        return bucketMetadataEntityMgr.getPublishedMetadataByModelGuidAndPublishedVersionFromReader(modelSummaryId,
                maxPublishedVersion);
    }

    @Override
    public List<BucketMetadata> getABCDBucketsByModelGuidAcrossTenant(String modelGuid) {
        return bucketMetadataEntityMgr.getUpToDateBucketMetadatasForModelFromReader(modelGuid);
    }

    @Override
    public List<BucketMetadata> getABCDBucketsByRatingEngineId(String ratingEngineId) {
        return bucketMetadataEntityMgr.getUpToDateBucketMetadatasForEngineFromReader(ratingEngineId);
    }

    @Override
    public List<BucketMetadata> getAllBucketsByRatingEngineId(String ratingEngineId) {
        return bucketMetadataEntityMgr.getAllBucketMetadatasForEngineFromReader(ratingEngineId);
    }

    @Override
    public List<BucketMetadata> getAllPublishedBucketsByRatingEngineId(String ratingEngineId) {
        return bucketMetadataEntityMgr.getAllPublishedBucketMetadatasForEngineFromReader(ratingEngineId);
    }

    @Override
    public Map<String, List<BucketMetadata>> getAllPublishedBucketMetadataByModelSummaryIdList(
            List<String> modelSummaryIdList) {
        Map<String, List<BucketMetadata>> modelSummaryToBucketListMap = new HashMap<>();
        if (CollectionUtils.isNotEmpty(modelSummaryIdList)) {
            for (String modelSummaryId : modelSummaryIdList) {
                modelSummaryToBucketListMap.put(modelSummaryId, getPublishedBucketMetadataByModelGuid(modelSummaryId));
            }
        }
        return modelSummaryToBucketListMap;
    }

    @Override
    public void createABCDBuckets(CreateBucketMetadataRequest request) {
        if (StringUtils.isBlank(request.getModelGuid())) {
            throw new IllegalArgumentException("Must specify model GUID");
        }
        List<BucketMetadata> bucketMetadataList = request.getBucketMetadataList();
        bucketMetadataList = BucketedScoreSummaryUtils.sortBucketMetadata(bucketMetadataList, false);
        long creationTimestamp = System.currentTimeMillis();
        Integer publishedVersion = getPublishedVersion(request.isPublished(), request.getModelGuid());

        bucketMetadataList.forEach(bucketMetadata -> {
            bucketMetadata.setCreationTimestamp(creationTimestamp);
            if (request.isCreateForModel()) {
                bucketMetadata.setOrigCreationTimestamp(creationTimestamp);
            }
            bucketMetadata.setLastModifiedByUser(request.getLastModifiedBy());
            bucketMetadata.setPublishedVersion(publishedVersion);
        });

        bucketMetadataEntityMgr.createBucketMetadata(bucketMetadataList, request.getModelGuid(),
                request.getRatingEngineId());
        if (StringUtils.isNotBlank(request.getModelGuid())) {
            modelSummaryService.updateLastUpdateTime(request.getModelGuid());
        }
        if (StringUtils.isNotBlank(request.getRatingEngineId())) {
            registerAction(request);
        }
    }

    private Integer getPublishedVersion(boolean published, String modelSummaryId) {
        if (published) {
            Integer maxPublishedVersion = bucketMetadataEntityMgr.getMaxPublishedVersionByModelId(modelSummaryId);
            if (maxPublishedVersion == null) {
                return 0;
            } else {
                return maxPublishedVersion + 1;
            }
        }
        return null;
    }

    @Override
    public List<BucketMetadata> updateABCDBuckets(UpdateBucketMetadataRequest request) {
        List<BucketMetadata> bucketMetadataList = request.getBucketMetadataList();
        String modelGuid = request.getModelGuid();
        List<BucketMetadata> updated = new ArrayList<>();
        bucketMetadataList = BucketedScoreSummaryUtils.sortBucketMetadata(bucketMetadataList, false);
        Integer publishedVersion = getPublishedVersion(request.isPublished(), request.getModelGuid());

        for (BucketMetadata bucketMetadata : bucketMetadataList) {
            if (bucketMetadata.getCreationTimestamp() <= 0) {
                throw new RuntimeException(
                        "Must specify meaningful creation timestamp for bucket metadata to be updated: "
                                + JsonUtils.serialize(request));
            }
            BucketMetadata existing = bucketMetadataEntityMgr.getBucketMetadatasByBucketNameAndTimestamp(
                    bucketMetadata.getBucketName(), bucketMetadata.getCreationTimestamp());
            if (existing != null) {
                existing.setNumLeads(bucketMetadata.getNumLeads());
                existing.setLift(bucketMetadata.getLift());
                existing.setPublishedVersion(publishedVersion);
                bucketMetadataEntityMgr.update(existing);
                if (existing.getModelSummary() != null && StringUtils.isNotBlank(existing.getModelSummary().getId())) {
                    modelGuid = existing.getModelSummary().getId();
                }
                updated.add(existing);
            } else {
                throw new RuntimeException("Cannot find existing bucket metadata to update");
            }
        }
        if (StringUtils.isNotBlank(modelGuid)) {
            modelSummaryService.updateLastUpdateTime(request.getModelGuid());
        }
        return updated;
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

    private void registerAction(CreateBucketMetadataRequest request) {
        String ratingEngineId = request.getRatingEngineId();
        String modelGuid = request.getModelGuid();
        String userId = request.getLastModifiedBy();
        log.info(String.format("Register AI_MODEL_BUCKET_CHANGE creation Action for RatingEngine %s, Model GUID %s",
                ratingEngineId, modelGuid));
        Action action = new Action();
        action.setType(ActionType.RATING_ENGINE_CHANGE);
        action.setActionInitiator(userId);
        RatingEngineActionConfiguration actionConfiguration = new RatingEngineActionConfiguration();
        actionConfiguration.setSubType(RatingEngineActionConfiguration.SubType.AI_MODEL_BUCKET_CHANGE);
        actionConfiguration.setRatingEngineId(ratingEngineId);
        actionConfiguration.setModelId(modelGuid);
        action.setActionConfiguration(actionConfiguration);
        action.setDescription(action.getActionConfiguration().serialize());
        log.debug(String.format("Registering action %s", action));
        actionService.create(action);
    }

}
