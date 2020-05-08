package com.latticeengines.pls.service.impl;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.ModelingQueryType;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.NoteParams;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineDashboard;
import com.latticeengines.domain.exposed.pls.RatingEngineNote;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingModelWithPublishedHistoryDTO;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQueryConstants;
import com.latticeengines.domain.exposed.ratings.coverage.ProductsCoverageRequest;
import com.latticeengines.domain.exposed.ratings.coverage.RatingEnginesCoverageRequest;
import com.latticeengines.domain.exposed.ratings.coverage.RatingEnginesCoverageResponse;
import com.latticeengines.domain.exposed.ratings.coverage.RatingsCountRequest;
import com.latticeengines.domain.exposed.ratings.coverage.RatingsCountResponse;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.RestrictionUtils;
import com.latticeengines.pls.service.RatingEngineService;
import com.latticeengines.proxy.exposed.cdl.RatingCoverageProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineDashboardProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;

@Component("ratingEngineService")
public class RatingEngineServiceImpl implements RatingEngineService {

    private static final Logger log = LoggerFactory.getLogger(RatingEngineServiceImpl.class);

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Inject
    private RatingEngineDashboardProxy ratingEngineDashboardProxy;

    @Inject
    private RatingCoverageProxy ratingCoverageProxy;

    @Override
    public List<RatingEngineSummary> getRatingEngineSummaries(String status, String type, Boolean publishedRatingsOnly) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingEngineProxy.getRatingEngineSummaries(tenant.getId(), status, type, publishedRatingsOnly);
    }

    @Override
    public List<RatingEngine> getAllDeletedRatingEngines() {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingEngineProxy.getAllDeletedRatingEngines(tenant.getId());
    }

    @Override
    public RatingEngine getRatingEngine(String ratingEngineId) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingEngineProxy.getRatingEngine(tenant.getId(), ratingEngineId);
    }

    @Override
    public DataPage getEntityPreview(String ratingEngineId, long offset, long maximum, BusinessEntity entityType,
                                     String sortBy, String bucketFieldName, Boolean descending, List<String> lookupFieldNames,
                                     Boolean restrictNotNullSalesforceId, String freeFormTextSearch, String freeFormTextSearch2,
                                     List<String> selectedBuckets, String lookupIdColumn) {
        descending = descending == null ? false : descending;
        Tenant tenant = MultiTenantContext.getTenant();
        if (StringUtils.isBlank(freeFormTextSearch) && StringUtils.isNotBlank(freeFormTextSearch2)) {
            freeFormTextSearch = freeFormTextSearch2;
        }
        return ratingEngineProxy.getEntityPreview(tenant.getId(), ratingEngineId, offset, maximum, entityType, sortBy,
                descending, bucketFieldName, lookupFieldNames, restrictNotNullSalesforceId, freeFormTextSearch,
                selectedBuckets, lookupIdColumn);
    }

    @Override
    public Long getEntityPreviewCount(String ratingEngineId, BusinessEntity entityType, Boolean restrictNotNullSalesforceId,
                                      String freeFormTextSearch, List<String> selectedBuckets, String lookupIdColumn) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingEngineProxy.getEntityPreviewCount(tenant.getId(), ratingEngineId, entityType,
                restrictNotNullSalesforceId, freeFormTextSearch, selectedBuckets, lookupIdColumn);
    }

    @Override
    public RatingEngineDashboard getRatingEngineDashboardById(String ratingEngineId) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingEngineDashboardProxy.getRatingEngineDashboardById(tenant.getId(), ratingEngineId);
    }

    @Override
    public List<RatingModelWithPublishedHistoryDTO> getPublishedHistory(String ratingEngineId) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingEngineProxy.getPublishedHistory(tenant.getId(), ratingEngineId);
    }

    @Override
    public RatingEngine createOrUpdateRatingEngine(RatingEngine ratingEngine, Boolean unlinkSegment, Boolean createAction) {
        Tenant tenant = MultiTenantContext.getTenant();
        String user = MultiTenantContext.getEmailAddress();
        ratingEngine.setUpdatedBy(user);
        cleanupBucketsInRules(ratingEngine);
        return ratingEngineProxy.createOrUpdateRatingEngine(tenant.getId(), ratingEngine, user, unlinkSegment,
                createAction);
    }

    @Override
    public Boolean deleteRatingEngine(String ratingEngineId, Boolean hardDelete) {
        Tenant tenant = MultiTenantContext.getTenant();
        ratingEngineProxy.deleteRatingEngine(tenant.getId(), ratingEngineId, hardDelete, MultiTenantContext.getEmailAddress());
        return true;
    }

    @Override
    public Boolean revertDeleteRatingEngine(String ratingEngineId) {
        Tenant tenant = MultiTenantContext.getTenant();
        ratingEngineProxy.revertDeleteRatingEngine(tenant.getId(), ratingEngineId);
        return true;
    }

    @Override
    public RatingsCountResponse getRatingEngineCoverageInfo(RatingsCountRequest ratingModelSegmentIds) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingCoverageProxy.getCoverageInfo(tenant.getId(), ratingModelSegmentIds);
    }

    @Override
    public RatingEnginesCoverageResponse getRatingEngineCoverageInfo(String segmentName,
                                                                     RatingEnginesCoverageRequest ratingEnginesCoverageRequest) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingCoverageProxy.getCoverageInfoForSegment(tenant.getId(), segmentName, ratingEnginesCoverageRequest);
    }

    @Override
    public RatingEnginesCoverageResponse getProductCoverageInfoForSegment(Integer purchasedBeforePeriod,
                                                                          ProductsCoverageRequest productsCoverageRequest) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingCoverageProxy.getProductCoverageInfoForSegment(tenant.getId(), productsCoverageRequest, purchasedBeforePeriod);
    }

    @Override
    public List<RatingModel> getRatingModels(String ratingEngineId) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingEngineProxy.getRatingModels(tenant.getId(), ratingEngineId);
    }

    @Override
    public RatingModel createModelIteration(String ratingEngineId, RatingModel ratingModel) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingEngineProxy.createModelIteration(tenant.getId(), ratingEngineId, ratingModel);
    }

    @Override
    public RatingModel getRatingModel(String ratingEngineId, String ratingModelId) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingEngineProxy.getRatingModel(tenant.getId(), ratingEngineId, ratingModelId);
    }

    @Override
    public RatingModel updateRatingModel(RatingModel ratingModel, String ratingEngineId, String ratingModelId) {
        Tenant tenant = MultiTenantContext.getTenant();
        String user = MultiTenantContext.getEmailAddress();
        cleanupBucketsInRules(ratingModel);
        return ratingEngineProxy.updateRatingModel(tenant.getId(), ratingEngineId, ratingModelId, ratingModel, user);

    }

    @Override
    public List<ColumnMetadata> getIterationMetadata(String ratingEngineId, String ratingModelId, String dataStores) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingEngineProxy.getIterationMetadata(tenant.getId(), ratingEngineId, ratingModelId, dataStores);
    }

    @Override
    public Map<String, StatsCube> getIterationMetadataCube(String ratingEngineId, String ratingModelId, String dataStores) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingEngineProxy.getIterationMetadataCube(tenant.getId(), ratingEngineId, ratingModelId, dataStores);

    }

    @Override
    public TopNTree getIterationMetadataTopN(String ratingEngineId, String ratingModelId, String dataStores) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingEngineProxy.getIterationMetadataTopN(tenant.getId(), ratingEngineId, ratingModelId, dataStores);
    }

    @Override
    public void setScoringIteration(String ratingEngineId, String ratingModelId, List<BucketMetadata> bucketMetadatas) {
        Tenant tenant = MultiTenantContext.getTenant();
        ratingEngineProxy.setScoringIteration(tenant.getId(), ratingEngineId, ratingModelId, bucketMetadatas,
                MultiTenantContext.getEmailAddress());
    }

    @Override
    public List<RatingEngineNote> getAllNotes(String ratingEngineId) {
        Tenant tenant = MultiTenantContext.getTenant();
        log.info(String.format("get all ratingEngineNotes by ratingEngineId=%s, tenant=%s", ratingEngineId,
                tenant.getId()));
        return ratingEngineProxy.getAllNotes(tenant.getId(), ratingEngineId);
    }

    @Override
    public Map<String, List<String>> getRatingEngineDependencies(String ratingEngineId) {
        Tenant tenant = MultiTenantContext.getTenant();
        log.info(String.format("get all ratingEngine dependencies for ratingEngineId=%s", ratingEngineId));
        return ratingEngineProxy.getRatingEngineDependencies(tenant.getId(), ratingEngineId);
    }

    @Override
    public Map<String, List<String>> getRatingEnigneDependenciesModelAndView(String ratingEngineId) {
        Tenant tenant = MultiTenantContext.getTenant();
        log.info(String.format("get all ratingEngine dependencies for ratingEngineId=%s", ratingEngineId));
        return ratingEngineProxy.getRatingEngineDependencies(tenant.getId(), ratingEngineId);
    }

    @Override
    public Boolean createNote(String ratingEngineId, NoteParams noteParams) {
        Tenant tenant = MultiTenantContext.getTenant();
        log.info(String.format("RatingEngineId=%s's note createdUser=%s, tenant=%s", ratingEngineId,
                noteParams.getUserName(), tenant.getId()));
        return ratingEngineProxy.createNote(tenant.getId(), ratingEngineId, noteParams);
    }

    @Override
    public void deleteNote(String ratingEngineId, String noteId) {
        Tenant tenant = MultiTenantContext.getTenant();
        log.info(String.format("RatingEngineNoteId=%s deleted by user=%s, tenant=%s", noteId,
                MultiTenantContext.getEmailAddress(), tenant.getId()));
        ratingEngineProxy.deleteNote(tenant.getId(), ratingEngineId, noteId);
    }

    @Override
    public Boolean updateNote(String ratingEngineId, String noteId, NoteParams noteParams) {
        Tenant tenant = MultiTenantContext.getTenant();
        log.info(String.format("RatingEngineNoteId=%s update by %s, tenant=%s", noteId, noteParams.getUserName(),
                tenant.getId()));
        return ratingEngineProxy.updateNote(tenant.getId(), ratingEngineId, noteId, noteParams);
    }

    @Override
    public EventFrontEndQuery getModelingQuery(String ratingEngineId, String ratingModelId,
                                               ModelingQueryType modelingQueryType, RatingEngine ratingEngine) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingEngineProxy.getModelingQueryByRating(tenant.getId(), ratingEngineId, ratingModelId,
                modelingQueryType, ratingEngine);
    }

    @Override
    public Long getModelingQueryCount(String ratingEngineId, String ratingModelId, ModelingQueryType modelingQueryType,
                                      RatingEngine ratingEngine) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingEngineProxy.getModelingQueryCountByRating(tenant.getId(), ratingEngineId, ratingModelId,
                modelingQueryType, ratingEngine);
    }

    @Override
    public EventFrontEndQuery getModelingQueryByRatingId(String ratingEngineId, String ratingModelId,
                                                         ModelingQueryType modelingQueryType) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingEngineProxy.getModelingQueryByRatingId(tenant.getId(), ratingEngineId, ratingModelId, modelingQueryType);
    }

    @Override
    public Long getModelingQueryCountByRatingId(String ratingEngineId, String ratingModelId, ModelingQueryType modelingQueryType) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingEngineProxy.getModelingQueryCountByRatingId(tenant.getId(), ratingEngineId, ratingModelId,
                modelingQueryType);
    }

    @Override
    public String ratingEngineModel(String ratingEngineId, String ratingModelId, List<ColumnMetadata> attributes, boolean skipValidation) {
        try {
            Tenant tenant = MultiTenantContext.getTenant();
            return ratingEngineProxy.modelRatingEngine(tenant.getId(), ratingEngineId, ratingModelId, attributes,
                    MultiTenantContext.getEmailAddress(), skipValidation);
        } catch (LedpException e) {
            throw e;
        } catch (Exception ex) {
            log.error("Failed to begin modeling job due to an unknown error!", ex);
            throw new RuntimeException(
                    "Failed to begin modeling job due to an unknown error, contact Lattice support for details!");
        }
    }

    @Override
    public boolean validateForModeling(String ratingEngineId, String ratingModelId) {
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingEngineProxy.validateForModelingByRatingEngineId(tenant.getId(), ratingEngineId, ratingModelId);
    }

    @Override
    public boolean validateForModeling(String ratingEngineId, String ratingModelId, RatingEngine ratingEngine) {
        RatingModel ratingModel = ratingEngine.getLatestIteration();
        if (ratingModel == null || !(ratingModel instanceof AIModel)) {
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[]{"LatestIteration of the given Model is Null or unsupported for validation"});
        }
        Tenant tenant = MultiTenantContext.getTenant();
        return ratingEngineProxy.validateForModeling(tenant.getId(), ratingEngineId, ratingModelId, ratingEngine);
    }

    private void cleanupBucketsInRules(RatingEngine re) {
        if (re != null) {
            cleanupBucketsInRules(re.getLatestIteration());
        }
    }

    private void cleanupBucketsInRules(RatingModel model) {
        if ((model instanceof RuleBasedModel)) {
            RuleBasedModel ruleBasedModel = (RuleBasedModel) model;
            if (ruleBasedModel.getRatingRule() != null) {
                TreeMap<String, Map<String, Restriction>> ruleMap = ruleBasedModel.getRatingRule().getBucketToRuleMap();
                if (MapUtils.isNotEmpty(ruleMap)) {
                    ruleMap.values().forEach(rules -> {
                        if (MapUtils.isNotEmpty(rules)) {
                            rules.values().forEach(RestrictionUtils::cleanupBucketsInRestriction);
                            rules.forEach((k, v) -> {
                                if (FrontEndQueryConstants.CONTACT_RESTRICTION.equals(k)) {
                                    RestrictionUtils.validateCentralEntity(v, BusinessEntity.Contact);
                                } else {
                                    RestrictionUtils.validateCentralEntity(v, BusinessEntity.Account);
                                }
                            });
                        }
                    });
                }
            }
        }
    }
}
