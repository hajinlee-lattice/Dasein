package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.apps.cdl.dao.AIModelDao;
import com.latticeengines.apps.cdl.dao.RatingEngineDao;
import com.latticeengines.apps.cdl.dao.RuleBasedModelDao;
import com.latticeengines.apps.cdl.entitymgr.PlayEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.RatingEngineEntityMgr;
import com.latticeengines.apps.cdl.repository.writer.RatingEngineRepository;
import com.latticeengines.apps.cdl.util.ActionContext;
import com.latticeengines.apps.core.annotation.SoftDeleteConfiguration;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.graph.traversal.impl.DepthFirstSearch;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.NoteOrigin;
import com.latticeengines.domain.exposed.pls.PlayStatus;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineActionConfiguration;
import com.latticeengines.domain.exposed.pls.RatingEngineNote;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.pls.cdl.rating.AdvancedRatingConfig;
import com.latticeengines.domain.exposed.pls.cdl.rating.CrossSellRatingConfig;
import com.latticeengines.domain.exposed.pls.cdl.rating.CustomEventRatingConfig;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.AdvancedModelingConfig;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CrossSellModelingConfig;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CustomEventModelingConfig;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.SubQueryAttrLookup;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQueryConstants;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("ratingEngineEntityMgr")
public class RatingEngineEntityMgrImpl extends BaseEntityMgrRepositoryImpl<RatingEngine, Long>
        implements RatingEngineEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(RatingEngineEntityMgrImpl.class);

    @Inject
    private RatingEngineDao ratingEngineDao;

    @Inject
    private RatingEngineRepository ratingEngineRepository;

    @Inject
    private RuleBasedModelDao ruleBasedModelDao;

    @Inject
    private AIModelDao aiModelDao;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private PlayEntityMgr playEntityMgr;

    @Override
    public BaseDao<RatingEngine> getDao() {
        return ratingEngineDao;
    }

    @Override
    public BaseJpaRepository<RatingEngine, Long> getRepository() {
        return ratingEngineRepository;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<RatingEngine> findAll() {
        return findAllByTypeAndStatus(null, null);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<RatingEngine> findAllByTypeAndStatus(String type, String status) {
        return ratingEngineDao.findAllByTypeAndStatus(type, status);
    }

    @Override
    @SoftDeleteConfiguration
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<RatingEngine> findAllDeleted() {
        return ratingEngineRepository.findByDeletedTrue();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<String> findAllIdsInSegment(String segmentName) {
        return ratingEngineDao.findAllIdsInSegment(segmentName);
    }

    @Override
    @SoftDeleteConfiguration
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public RatingEngine findById(String id) {
        return findById(id, false);
    }

    @Override
    @SoftDeleteConfiguration
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public RatingEngine findById(String id, boolean withActiveModel) {
        RatingEngine ratingEngine = ratingEngineDao.findById(id);
        if (withActiveModel && ratingEngine != null) {
            Long activeModelPid = ratingEngine.getActiveModelPid();
            if (activeModelPid != null) {
                switch (ratingEngine.getType()) {
                case RULE_BASED:
                    RatingModel ruleBasedModel = ruleBasedModelDao.findByKey(RuleBasedModel.class, activeModelPid);
                    ratingEngine.setActiveModel(ruleBasedModel);
                    break;
                case CUSTOM_EVENT:
                case CROSS_SELL:
                    RatingModel aiModel = aiModelDao.findByKey(AIModel.class, activeModelPid);
                    ratingEngine.setActiveModel(aiModel);
                    break;
                default:
                    break;
                }
            }
        }
        return ratingEngine;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteById(String id) {
        deleteById(id, true);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteRatingEngine(RatingEngine ratingEngine) {
        deleteRatingEngine(ratingEngine, true);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteById(String id, boolean hardDelete) {
        RatingEngine ratingEngine = findById(id);
        checkDependencies(ratingEngine);
        ratingEngineDao.deleteById(id, hardDelete);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteRatingEngine(RatingEngine ratingEngine, boolean hardDelete) {
        if (ratingEngine == null || ratingEngine.getPid() == null) {
            throw new NullPointerException("RatingEngine cannot be found");
        }
        checkDependencies(ratingEngine);
        ratingEngineDao.deleteByPid(ratingEngine.getPid(), hardDelete);
    }

    private void checkDependencies(RatingEngine ratingEngine) {
        if (CollectionUtils.isNotEmpty(playEntityMgr.findByRatingEngineAndPlayStatusIn(ratingEngine,
                Arrays.asList(PlayStatus.ACTIVE, PlayStatus.INACTIVE)))) {
            log.error(String.format("Dependency check failed for Rating Engine=%s", ratingEngine.getId()));
            throw new LedpException(LedpCode.LEDP_18175, new String[] { ratingEngine.getDisplayName() });
        }
        if (ratingEngine.getStatus() != null && ratingEngine.getStatus() != RatingEngineStatus.INACTIVE) {
            throw new LedpException(LedpCode.LEDP_18181, new String[] { ratingEngine.getDisplayName() });
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void revertDelete(String id) {
        ratingEngineDao.revertDeleteById(id);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public RatingEngine createOrUpdateRatingEngine(RatingEngine ratingEngine, String tenantId) {
        return createOrUpdateRatingEngine(ratingEngine, tenantId, false);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public RatingEngine createOrUpdateRatingEngine(RatingEngine ratingEngine, String tenantId, Boolean unlinkSegment) {
        if (ratingEngine.getId() == null) { // create a new Rating Engine
            ratingEngine.setId(RatingEngine.generateIdStr());
            createNewRatingEngine(ratingEngine, tenantId);
            return ratingEngine;
        } else {
            RatingEngine retrievedRatingEngine = findById(ratingEngine.getId());
            if (retrievedRatingEngine == null) {
                log.warn(String.format("Rating Engine with id %s for tenant %s cannot be found", ratingEngine.getId(),
                        tenantId));
                createNewRatingEngine(ratingEngine, tenantId);
                return ratingEngine;
            } else { // update an existing one by updating the delta passed from
                // front end
                updateExistingRatingEngine(retrievedRatingEngine, ratingEngine, tenantId, unlinkSegment);
                return retrievedRatingEngine;
            }
        }
    }

    private void updateExistingRatingEngine(RatingEngine retrievedRatingEngine, RatingEngine ratingEngine,
            String tenantId, Boolean unlinkSegment) {
        log.info(String.format("Updating existing rating engine with id %s for tenant %s", ratingEngine.getId(),
                tenantId));
        if (ratingEngine.getDisplayName() != null) {
            retrievedRatingEngine.setDisplayName(ratingEngine.getDisplayName());
        }
        if (ratingEngine.getSegment() != null) {
            retrievedRatingEngine.setSegment(ratingEngine.getSegment());
        }
        if (ratingEngine.getStatus() != null) {
            validateForStatusUpdate(retrievedRatingEngine, ratingEngine);
            // set Activation Action Context
            if (retrievedRatingEngine.getStatus() == RatingEngineStatus.INACTIVE
                    && ratingEngine.getStatus() == RatingEngineStatus.ACTIVE) {
                setActivationActionContext(retrievedRatingEngine);
                retrievedRatingEngine.setJustCreated(false);
            }
            retrievedRatingEngine.setStatus(ratingEngine.getStatus());
        }
        if (ratingEngine.getNote() != null) {
            RatingEngineNote ratingEngineNote = new RatingEngineNote();
            ratingEngineNote.setNotesContents(ratingEngine.getNote());
            ratingEngineNote.setCreatedByUser(ratingEngine.getCreatedBy());
            ratingEngineNote.setLastModifiedByUser(ratingEngine.getCreatedBy());

            Long nowTimestamp = (new Date()).getTime();
            ratingEngineNote.setCreationTimestamp(nowTimestamp);
            ratingEngineNote.setLastModificationTimestamp(nowTimestamp);
            ratingEngineNote.setRatingEngine(retrievedRatingEngine);
            ratingEngineNote.setOrigin(NoteOrigin.NOTE.name());
            ratingEngineNote.setId(UUID.randomUUID().toString());
            retrievedRatingEngine.addRatingEngineNote(ratingEngineNote);
        }
        if (MapUtils.isNotEmpty(ratingEngine.getCountsAsMap())) {
            retrievedRatingEngine.setCountsByMap(ratingEngine.getCountsAsMap());
        }
        if (ratingEngine.getAdvancedRatingConfig() != null) {
            if (retrievedRatingEngine.getAdvancedRatingConfig() != null) {
                retrievedRatingEngine.getAdvancedRatingConfig().copyConfig(ratingEngine.getAdvancedRatingConfig());
            } else {
                retrievedRatingEngine.setAdvancedRatingConfig(ratingEngine.getAdvancedRatingConfig());
            }
        }

        if (ratingEngine.getActiveModelPid() != null) {
            retrievedRatingEngine.setActiveModelPid(ratingEngine.getActiveModelPid());
        }

        // PLS-7555 - allow segment to be reset to null for custom event rating
        if (unlinkSegment == Boolean.TRUE && !retrievedRatingEngine.getType().isTargetSegmentMandatory()) {
            retrievedRatingEngine.setSegment(null);
        }
        retrievedRatingEngine.setUpdated(new Date());
        ratingEngineDao.update(retrievedRatingEngine);
    }

    @VisibleForTesting
    void validateForStatusUpdate(RatingEngine retrievedRatingEngine, RatingEngine ratingEngine) {
        // Check transition diagram
        if (!RatingEngineStatus.canTransit(retrievedRatingEngine.getStatus(), ratingEngine.getStatus())) {
            log.error(String.format("Status transition of the Rating Engine %s is invalid.",
                    retrievedRatingEngine.getId()));
            throw new LedpException(LedpCode.LEDP_18174, new String[] { retrievedRatingEngine.getStatus().name(),
                    ratingEngine.getStatus().name(), ratingEngine.getDisplayName() });
        }

        // Check active model of Rating Engine
        if (ratingEngine.getStatus() == RatingEngineStatus.ACTIVE) {
            if (retrievedRatingEngine.getActiveModelPid() == null) {
                log.error(
                        String.format("Active Model check failed for Rating Engine=%s", retrievedRatingEngine.getId()));
                throw new LedpException(LedpCode.LEDP_18175, new String[] { retrievedRatingEngine.getDisplayName() });
            }
        }
    }

    private void createNewRatingEngine(RatingEngine ratingEngine, String tenantId) {
        log.info(String.format("Creating a new Rating Engine entity with the id of %s for tenant %s",
                ratingEngine.getId(), tenantId));
        Tenant tenant = tenantEntityMgr.findByTenantId(tenantId);
        ratingEngine.setTenant(tenant);
        ratingEngine.setDisplayName(ratingEngine.generateDefaultName());
        RatingEngineType type = ratingEngine.getType();
        if (type == null) {
            throw new LedpException(LedpCode.LEDP_18154, new String[] { ratingEngine.toString() });
        }
        MetadataSegment segment = ratingEngine.getSegment();
        if (ratingEngine.getType().isTargetSegmentMandatory() && (segment == null || segment.getName() == null)) {
            throw new LedpException(LedpCode.LEDP_18153, new String[] { ratingEngine.toString() });
        }

        if (ratingEngine.getStatus() == null) {
            ratingEngine.setStatus(RatingEngineStatus.INACTIVE);
        }
        if (ratingEngine.getNote() != null) {
            RatingEngineNote ratingEngineNote = new RatingEngineNote();
            ratingEngineNote.setNotesContents(ratingEngine.getNote());
            ratingEngineNote.setCreatedByUser(ratingEngine.getCreatedBy());
            ratingEngineNote.setLastModifiedByUser(ratingEngine.getCreatedBy());

            Long nowTimestamp = (new Date()).getTime();
            ratingEngineNote.setCreationTimestamp(nowTimestamp);
            ratingEngineNote.setLastModificationTimestamp(nowTimestamp);
            ratingEngineNote.setRatingEngine(ratingEngine);
            ratingEngineNote.setOrigin(NoteOrigin.NOTE.name());
            ratingEngineNote.setId(UUID.randomUUID().toString());
            ratingEngine.addRatingEngineNote(ratingEngineNote);
        }

        AdvancedModelingConfig advancedModelingConfig = null;
        AdvancedRatingConfig advancedRatingConfig = ratingEngine.getAdvancedRatingConfig();
        switch (type) {
        case CROSS_SELL:
            if (advancedModelingConfig == null) {
                advancedModelingConfig = new CrossSellModelingConfig();
            }
            if (advancedRatingConfig == null) {
                advancedRatingConfig = new CrossSellRatingConfig();
            }
            break;
        case CUSTOM_EVENT:
            if (advancedModelingConfig == null) {
                advancedModelingConfig = new CustomEventModelingConfig();
            }
            if (advancedRatingConfig == null) {
                advancedRatingConfig = new CustomEventRatingConfig();
            }
            break;
        default:
            break;
        }

        ratingEngine.setAdvancedRatingConfig(advancedRatingConfig);

        // set Activation Action Context
        if (RatingEngineStatus.ACTIVE.equals(ratingEngine.getStatus())) {
            setActivationActionContext(ratingEngine);
            ratingEngine.setJustCreated(false);
        }
        ratingEngineDao.create(ratingEngine);

        if (type == RatingEngineType.RULE_BASED) {
            createRuleBasedModel(ratingEngine);
        } else {
            createAIModel(ratingEngine, advancedModelingConfig);
        }
    }

    private void createRuleBasedModel(RatingEngine ratingEngine) {
        RuleBasedModel ruleBasedModel = new RuleBasedModel();
        ruleBasedModel.setId(RuleBasedModel.generateIdStr());
        ruleBasedModel.setRatingEngine(ratingEngine);

        ruleBasedModel.setRatingRule(RatingRule.constructDefaultRule());
        List<String> usedAttributesInSegment = findUsedAttributes(ratingEngine.getSegment());
        ruleBasedModel.setSelectedAttributes(usedAttributesInSegment);

        populateUnusedAttrsInBktRestrictions(ratingEngine, ruleBasedModel);

        ruleBasedModelDao.create(ruleBasedModel);
        ratingEngine.setActiveModelPid(ruleBasedModel.getPid());
        ratingEngine.setActiveModel(ruleBasedModel);
        ratingEngineDao.update(ratingEngine);
    }

    private void populateUnusedAttrsInBktRestrictions(RatingEngine ratingEngine, RuleBasedModel ruleBasedModel) {
        Map<String, Map<String, Restriction>> bucketToRuleMap = //
                ruleBasedModel.getRatingRule().getBucketToRuleMap();

        if (ratingEngine.getSegment() != null) {
            populateUnusedAttrsInBktRestrictions(ratingEngine, bucketToRuleMap,
                    ratingEngine.getSegment().getAccountRestriction(), FrontEndQueryConstants.ACCOUNT_RESTRICTION);

            populateUnusedAttrsInBktRestrictions(ratingEngine, bucketToRuleMap,
                    ratingEngine.getSegment().getContactRestriction(), FrontEndQueryConstants.CONTACT_RESTRICTION);
        }
    }

    private void populateUnusedAttrsInBktRestrictions(RatingEngine ratingEngine,
            Map<String, Map<String, Restriction>> bucketToRuleMap, Restriction partialSegmentRestriction, String type) {
        Set<String> attributesPartialRestrictionInSegment = new HashSet<>();
        traverseAndRestriction(attributesPartialRestrictionInSegment, partialSegmentRestriction);

        List<Restriction> unusedAttributeRestrictions;
        Restriction bucketAccountRestriction;

        if (CollectionUtils.isNotEmpty(attributesPartialRestrictionInSegment)) {
            unusedAttributeRestrictions = //
                    attributesPartialRestrictionInSegment.stream() //
                            .map(attr -> {
                                BucketRestriction unusedAttrRestriction = new BucketRestriction(
                                        AttributeLookup.fromString(attr), Bucket.nullBkt());
                                unusedAttrRestriction.setIgnored(true);
                                return unusedAttrRestriction;
                            }) //
                            .collect(Collectors.toList());

        } else {
            unusedAttributeRestrictions = new ArrayList<>();
        }

        bucketAccountRestriction = LogicalRestriction.builder().and(unusedAttributeRestrictions).build();

        bucketToRuleMap.keySet() //
                .forEach(k -> bucketToRuleMap.get(k) //
                        .put(type, bucketAccountRestriction));
    }

    private void createAIModel(RatingEngine ratingEngine, AdvancedModelingConfig advancedModelingConfig) {
        AIModel aiModel = new AIModel();
        aiModel.setId(AIModel.generateIdStr());
        aiModel.setRatingEngine(ratingEngine);
        aiModel.setAdvancedModelingConfig(advancedModelingConfig);
        aiModelDao.create(aiModel);
        ratingEngine.setActiveModelPid(aiModel.getPid());
        ratingEngine.setActiveModel(aiModel);
        ratingEngineDao.update(ratingEngine);

    }

    private void setActivationActionContext(RatingEngine ratingEngine) {
        log.info(String.format("Set Activation Action Context for Rating Engine %s", ratingEngine.getId()));
        Action ratingEngineActivateAction = new Action();
        ratingEngineActivateAction.setType(ActionType.RATING_ENGINE_CHANGE);
        ratingEngineActivateAction.setActionInitiator(ratingEngine.getCreatedBy());
        RatingEngineActionConfiguration reActionConfig = new RatingEngineActionConfiguration();
        reActionConfig.setRatingEngineId(ratingEngine.getId());
        reActionConfig.setSubType(RatingEngineActionConfiguration.SubType.ACTIVATION);
        ratingEngineActivateAction.setActionConfiguration(reActionConfig);
        ActionContext.setAction(ratingEngineActivateAction);
    }

    @VisibleForTesting
    List<String> findUsedAttributes(MetadataSegment segment) {
        Set<String> usedAttributesSetInSegment = new HashSet<>();

        if (segment != null) {
            traverseAndRestriction(usedAttributesSetInSegment, segment.getAccountRestriction());
            traverseAndRestriction(usedAttributesSetInSegment, segment.getContactRestriction());
        }

        return new ArrayList<>(usedAttributesSetInSegment);
    }

    private void traverseAndRestriction(Set<String> usedAttributesInSegment, Restriction restriction) {
        if (restriction != null) {
            DepthFirstSearch search = new DepthFirstSearch();
            search.run(restriction, (object, ctx) -> {
                GraphNode node = (GraphNode) object;
                if (node instanceof ConcreteRestriction) {
                    ConcreteRestriction cr = (ConcreteRestriction) node;
                    Lookup lookup = cr.getLhs();
                    if (lookup instanceof AttributeLookup) {
                        usedAttributesInSegment.add(sanitize((lookup).toString()));
                    } else if (lookup instanceof SubQueryAttrLookup) {
                        usedAttributesInSegment.add(sanitize(((SubQueryAttrLookup) lookup).getAttribute()));
                    }
                } else if (node instanceof BucketRestriction) {
                    BucketRestriction bucket = (BucketRestriction) node;
                    usedAttributesInSegment.add(sanitize(bucket.getAttr().toString()));
                }
            });
        }
    }

    private String sanitize(String attribute) {
        if (StringUtils.isNotBlank(attribute)) {
            attribute = attribute.trim();
        }
        return attribute;
    }
}
