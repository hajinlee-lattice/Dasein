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

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.apps.cdl.dao.AIModelDao;
import com.latticeengines.apps.cdl.dao.RatingEngineDao;
import com.latticeengines.apps.cdl.dao.RuleBasedModelDao;
import com.latticeengines.apps.cdl.entitymgr.GraphVisitable;
import com.latticeengines.apps.cdl.entitymgr.GraphVisitor;
import com.latticeengines.apps.cdl.entitymgr.PlayEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.RatingEngineEntityMgr;
import com.latticeengines.apps.cdl.repository.RatingEngineRepository;
import com.latticeengines.apps.cdl.util.ActionContext;
import com.latticeengines.apps.core.annotation.SoftDeleteConfiguration;
import com.latticeengines.apps.core.service.ActionService;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.graph.traversal.impl.DepthFirstSearch;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.graph.EdgeType;
import com.latticeengines.domain.exposed.graph.ParsedDependencies;
import com.latticeengines.domain.exposed.graph.VertexType;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.modeling.CustomEventModelingType;
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
public class RatingEngineEntityMgrImpl //
        extends BaseReadWriteRepoEntityMgrImpl<RatingEngineRepository, RatingEngine, Long> //
        implements RatingEngineEntityMgr, GraphVisitable {

    private static final Logger log = LoggerFactory.getLogger(RatingEngineEntityMgrImpl.class);

    @Inject
    private RatingEngineEntityMgrImpl _self;

    @Inject
    private RatingEngineDao ratingEngineDao;

    @Resource(name = "ratingEngineWriterRepository")
    private RatingEngineRepository ratingEngineWriterRepository;

    @Resource(name = "ratingEngineReaderRepository")
    private RatingEngineRepository ratingEngineReaderRepository;

    @Inject
    private RuleBasedModelDao ruleBasedModelDao;

    @Inject
    private AIModelDao aiModelDao;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private PlayEntityMgr playEntityMgr;

    @Inject
    private ActionService actionService;

    @Override
    public BaseDao<RatingEngine> getDao() {
        return ratingEngineDao;
    }

    @Override
    protected RatingEngineRepository getReaderRepo() {
        return ratingEngineReaderRepository;
    }

    @Override
    protected RatingEngineRepository getWriterRepo() {
        return ratingEngineWriterRepository;
    }

    @Override
    protected RatingEngineEntityMgrImpl getSelf() {
        return _self;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<RatingEngine> findAll() {
        return ratingEngineDao.findAll();
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<RatingEngine> findAllByTypeAndStatus(String type, String status) {
        if (isReaderConnection()) {
            return findAllByTypeAndStatusFromReader(type, status);
        } else {
            return getAllByTypeAndStatusFromRepo(getWriterRepo(), type, status);
        }
    }

    @Transactional(transactionManager = "transactionManagerReader", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<RatingEngine> findAllByTypeAndStatusFromReader(String type, String status) {
        return getAllByTypeAndStatusFromRepo(getReaderRepo(), type, status);
    }

    private List<RatingEngine> getAllByTypeAndStatusFromRepo(RatingEngineRepository repo, String type, String status) {
        if (type == null && status == null) {
            return repo.findAll();
        } else if (type == null) {
            return repo.findByStatus(RatingEngineStatus.valueOf(status));
        } else if (status == null) {
            return repo.findByType(RatingEngineType.valueOf(type));
        } else {
            return repo.findByTypeAndStatus(RatingEngineType.valueOf(type), RatingEngineStatus.valueOf(status));
        }
    }

    @Override
    @SoftDeleteConfiguration
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<RatingEngine> findAllDeleted() {
        return ratingEngineWriterRepository.findByDeletedTrue();
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

    @SuppressWarnings("deprecation")
    @Override
    @SoftDeleteConfiguration
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public RatingEngine findById(String id, boolean inflate) {
        return ratingEngineDao.findById(id);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteById(String id, boolean hardDelete, String actionInitiator) {
        checkFeasibilityForDelete(id);
        ratingEngineDao.deleteById(id, hardDelete);
        registerDeleteAction(id, actionInitiator);
    }

    private void checkFeasibilityForDelete(String id) {
        if (StringUtils.isBlank(id)) {
            throw new NullPointerException("RatingEngine id cannot be empty");
        }
        RatingEngine ratingEngine = findById(id);
        if (ratingEngine == null || ratingEngine.getPid() == null) {
            throw new NullPointerException("RatingEngine cannot be found");
        }

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

    @SuppressWarnings("deprecation")
    private void updateExistingRatingEngine(RatingEngine retrievedRatingEngine, RatingEngine ratingEngine,
            String tenantId, Boolean unlinkSegment) {
        log.info(String.format("Updating existing rating engine with id %s for tenant %s", ratingEngine.getId(),
                tenantId));
        if (ratingEngine.getDisplayName() != null) {
            retrievedRatingEngine.setDisplayName(ratingEngine.getDisplayName());
        }
        if (ratingEngine.getDescription() != null) {
            retrievedRatingEngine.setDescription(ratingEngine.getDescription());
        }
        if (ratingEngine.getStatus() != null) {
            validateForStatusUpdate(retrievedRatingEngine, ratingEngine);
            // set Activation Action Context
            if (retrievedRatingEngine.getStatus() == RatingEngineStatus.INACTIVE
                    && ratingEngine.getStatus() == RatingEngineStatus.ACTIVE) {
                setActivationActionContext(retrievedRatingEngine);
                if (retrievedRatingEngine.getScoringIteration() == null) {
                    if (retrievedRatingEngine.getType() == RatingEngineType.RULE_BASED) {
                        retrievedRatingEngine.setScoringIteration(retrievedRatingEngine.getLatestIteration());
                    } else {
                        log.error(String.format("No scoring iteration set for Rating Engine: %s",
                                retrievedRatingEngine.getId()));
                        throw new LedpException(LedpCode.LEDP_18186,
                                new String[] { retrievedRatingEngine.getDisplayName() });
                    }
                }
            }
            retrievedRatingEngine.setStatus(ratingEngine.getStatus());
        }
        if (ratingEngine.getNote() != null) {
            RatingEngineNote ratingEngineNote = new RatingEngineNote();
            ratingEngineNote.setNotesContents(ratingEngine.getNote());
            ratingEngineNote.setCreatedByUser(ratingEngine.getCreatedBy());
            ratingEngineNote.setLastModifiedByUser(ratingEngine.getUpdatedBy());

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

        if (ratingEngine.getLatestIteration() != null) {
            retrievedRatingEngine.setLatestIteration(ratingEngine.getLatestIteration());
        }

        if (ratingEngine.getScoringIteration() != null) {
            retrievedRatingEngine.setScoringIteration(ratingEngine.getScoringIteration());
        }

        if (ratingEngine.getPublishedIteration() != null) {
            retrievedRatingEngine.setPublishedIteration(ratingEngine.getPublishedIteration());
        }

        if (ratingEngine.getSegment() != null) {
            retrievedRatingEngine.setSegment(ratingEngine.getSegment());
            if (retrievedRatingEngine.getType() == RatingEngineType.CUSTOM_EVENT) {
                updateCustomEventModelingType(retrievedRatingEngine, CustomEventModelingType.CDL);
            }
        }

        // PLS-7555 - allow segment to be reset to null for custom event rating
        if (unlinkSegment == Boolean.TRUE && !retrievedRatingEngine.getType().isTargetSegmentMandatory()) {
            retrievedRatingEngine.setSegment(null);
            if (retrievedRatingEngine.getType() == RatingEngineType.CUSTOM_EVENT) {
                updateCustomEventModelingType(retrievedRatingEngine, CustomEventModelingType.LPI);
            }
        }

        retrievedRatingEngine.setUpdated(new Date());
        log.info("================\n\n" + JsonUtils.serialize(retrievedRatingEngine) + "\n\n");
        ratingEngineDao.update(retrievedRatingEngine);
    }

    void updateCustomEventModelingType(RatingEngine retrievedRatingEngine, CustomEventModelingType modelingType) {
        AIModel model = (AIModel) retrievedRatingEngine.getLatestIteration();
        CustomEventModelingConfig advancedModelingConfig = (CustomEventModelingConfig) model
                .getAdvancedModelingConfig();
        advancedModelingConfig.setCustomEventModelingType(modelingType);
    }

    @VisibleForTesting
    void validateForStatusUpdate(RatingEngine retrievedRatingEngine, RatingEngine ratingEngine) {
        // Check transition diagram
        if (!retrievedRatingEngine.getStatus().canTransition(ratingEngine.getStatus())) {
            log.error(String.format("Cannot Transition Rating Engine: %s from %s to %s.", //
                    retrievedRatingEngine.getId(), retrievedRatingEngine.getStatus().name(),
                    ratingEngine.getStatus().name()));
            throw new LedpException(LedpCode.LEDP_18174, new String[] { ratingEngine.getDisplayName(),
                    retrievedRatingEngine.getStatus().name(), ratingEngine.getStatus().name(), });
        }

        // Check active model of Rating Engine
        if (ratingEngine.getStatus() == RatingEngineStatus.ACTIVE
                && retrievedRatingEngine.getType() != RatingEngineType.RULE_BASED
                && retrievedRatingEngine.getScoringIteration() == null) {
            log.error(String.format("No scoring iteration set for Rating Engine: %s", retrievedRatingEngine.getId()));
            throw new LedpException(LedpCode.LEDP_18186, new String[] { retrievedRatingEngine.getDisplayName() });
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
            ratingEngineNote.setLastModifiedByUser(ratingEngine.getUpdatedBy());

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
                CustomEventModelingType modelingType = segment == null ? CustomEventModelingType.LPI
                        : CustomEventModelingType.CDL;
                ((CustomEventModelingConfig) advancedModelingConfig).setCustomEventModelingType(modelingType);
            }
            if (advancedRatingConfig == null) {
                advancedRatingConfig = new CustomEventRatingConfig();
            }
            break;
        default:
            break;
        }

        ratingEngine.setAdvancedRatingConfig(advancedRatingConfig);

        ratingEngineDao.create(ratingEngine);

        if (type == RatingEngineType.RULE_BASED) {
            createRuleBasedModel(ratingEngine);
        } else {
            createAIModel(ratingEngine, advancedModelingConfig);
        }

        // set Activation Action Context
        if (RatingEngineStatus.ACTIVE == ratingEngine.getStatus()) {
            if (ratingEngine.getScoringIteration() == null) {
                if (ratingEngine.getType() != RatingEngineType.RULE_BASED) {
                    log.error(String.format("No scoring iteration set for Rating Engine: %s", ratingEngine.getId()));
                    throw new LedpException(LedpCode.LEDP_18186, new String[] { ratingEngine.getDisplayName() });
                } else {
                    ratingEngine.setScoringIteration(ratingEngine.getLatestIteration());
                    ratingEngineDao.update(ratingEngine);
                }
            }
            setActivationActionContext(ratingEngine);
        }
    }

    @SuppressWarnings("deprecation")
    private void createRuleBasedModel(RatingEngine ratingEngine) {
        RuleBasedModel ruleBasedModel = new RuleBasedModel();
        ruleBasedModel.setId(RuleBasedModel.generateIdStr());
        ruleBasedModel.setRatingEngine(ratingEngine);
        ruleBasedModel.setCreatedBy(ratingEngine.getCreatedBy());
        ruleBasedModel.setUpdatedBy(ratingEngine.getUpdatedBy());

        ruleBasedModel.setRatingRule(RatingRule.constructDefaultRule());
        List<String> usedAttributesInSegment = findUsedAttributes(ratingEngine.getSegment());
        ruleBasedModel.setSelectedAttributes(usedAttributesInSegment);

        populateUnusedAttrsInBktRestrictions(ratingEngine, ruleBasedModel);

        ruleBasedModelDao.create(ruleBasedModel);

        ratingEngine.setLatestIteration(ruleBasedModel);
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

    @SuppressWarnings("deprecation")
    private void createAIModel(RatingEngine ratingEngine, AdvancedModelingConfig advancedModelingConfig) {
        AIModel aiModel = new AIModel();
        aiModel.setId(AIModel.generateIdStr());
        aiModel.setCreatedBy(ratingEngine.getCreatedBy());
        aiModel.setUpdatedBy(ratingEngine.getUpdatedBy());
        aiModel.setRatingEngine(ratingEngine);
        aiModel.setAdvancedModelingConfig(advancedModelingConfig);
        aiModelDao.create(aiModel);

        ratingEngine.setLatestIteration(aiModel);
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

    private void registerDeleteAction(String ratingEngineId, String actionInitiator) {
        log.info(String.format("Register Deletion Action for Rating Engine %s", ratingEngineId));
        Action ratingEngineDeletionAction = new Action();
        ratingEngineDeletionAction.setType(ActionType.RATING_ENGINE_CHANGE);
        ratingEngineDeletionAction.setActionInitiator(actionInitiator);
        ratingEngineDeletionAction.setTenant(MultiTenantContext.getTenant());
        RatingEngineActionConfiguration reActionConfig = new RatingEngineActionConfiguration();
        reActionConfig.setHiddenFromUI(true);
        reActionConfig.setRatingEngineId(ratingEngineId);
        reActionConfig.setSubType(RatingEngineActionConfiguration.SubType.DELETION);
        ratingEngineDeletionAction.setActionConfiguration(reActionConfig);
        actionService.create(ratingEngineDeletionAction);
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

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public RatingEngine createRatingEngine(RatingEngine ratingEngine) {
        String tenantId = MultiTenantContext.getTenant().getId();
        createNewRatingEngine(ratingEngine, tenantId);
        return findById(ratingEngine.getId());
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public RatingEngine updateRatingEngine(RatingEngine ratingEngine, RatingEngine retrievedRatingEngine,
            Boolean unlinkSegment) {
        String tenantId = MultiTenantContext.getTenant().getId();
        retrievedRatingEngine = findById(retrievedRatingEngine.getId());
        updateExistingRatingEngine(retrievedRatingEngine, ratingEngine, tenantId, unlinkSegment);
        log.info(retrievedRatingEngine.toString());
        return retrievedRatingEngine;
    }

    @Override
    public Set<Triple<String, String, String>> extractDependencies(RatingEngine ratingEngine) {
        Set<Triple<String, String, String>> attrDepSet = null;
        if (ratingEngine != null && ratingEngine.getSegment() != null) {
            String targetSegmentName = ratingEngine.getSegment().getName();
            attrDepSet = new HashSet<>();

            attrDepSet.add(ParsedDependencies.tuple(targetSegmentName, //
                    VertexType.SEGMENT, EdgeType.DEPENDS_ON_FOR_TARGET));
        }
        if (CollectionUtils.isNotEmpty(attrDepSet)) {
            log.info(String.format("Extracted dependencies from rating engine %s: %s", ratingEngine.getId(),
                    JsonUtils.serialize(attrDepSet)));
        }
        return attrDepSet;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public void accept(GraphVisitor visitor, Object entity) throws Exception {
        visitor.visit((RatingEngine) entity, parse((RatingEngine) entity, null));
    }
}
