package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.GraphVisitor;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.graph.ParsedDependencies;
import com.latticeengines.domain.exposed.graph.VertexCreationRequest;
import com.latticeengines.domain.exposed.graph.VertexType;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngine.ScoreType;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.graphdb.BootstrapContext;
import com.latticeengines.graphdb.entity.GraphEntityManager;

@Component
public class GraphVisitorImpl implements GraphVisitor {

    private static final Logger log = LoggerFactory.getLogger(GraphVisitorImpl.class);

    @Inject
    private GraphEntityManager graphEntityManager;

    @Inject
    private BootstrapContext bootstrapContext;

    @Inject
    private SegmentEntityMgrImpl segmentEntityMgr;

    @Inject
    private RatingEngineEntityMgrImpl ratingEngineEntityMgr;

    @Inject
    private PlayEntityMgrImpl playEntityMgr;

    @Inject
    private AIModelEntityMgrImpl aiModelEntityMgrImpl;

    @Inject
    private RuleBasedModelEntityMgrImpl ruleBasedModelEntityMgrImpl;

    @Inject
    private RatingAttributeNameParser ratingAttributeNameParser;

    @Inject
    private CDLDependenciesToGraphAction cdlGraphAction;

    @Inject
    private VisitorBook visitorBook;

    @Override
    public void populateTenantGraph(Tenant tenant) throws Exception {
        Tenant originalTenant = MultiTenantContext.getTenant();
        Long bootstrapId = System.currentTimeMillis();
        try {
            visitorBook.initVisitorBook();
            log.info(String.format("[%d] Dependency graph bootstrap for tenant '%s' - started", bootstrapId,
                    tenant.getId()));
            MultiTenantContext.setTenant(tenant);
            bootstrapContext.initThreadLocalVertexExistenceSet();
            createTenantVertex();
            log.info(String.format("[%d] Dependency graph bootstrap for tenant '%s' - Step 1/3 - %s", bootstrapId,
                    tenant.getId(), "traverseSegments"));
            traverseSegments(segmentEntityMgr);
            log.info(String.format("[%d] Dependency graph bootstrap for tenant '%s' - Step 2/3 - %s", bootstrapId,
                    tenant.getId(), "traverseRatingEngines"));
            traverseRatingEngines(ratingEngineEntityMgr);
            log.info(String.format("[%d] Dependency graph bootstrap for tenant '%s' - Step 3/3 - %s", bootstrapId,
                    tenant.getId(), "traversePlays"));
            traversePlays(playEntityMgr);
            log.info(String.format("[%d] Dependency graph bootstrap for tenant '%s' - completed", bootstrapId,
                    tenant.getId()));
        } catch (Throwable th) {
            log.error(String.format("[%d] Dependency graph bootstrap for tenant '%s' - failed", bootstrapId,
                    tenant.getId()), th);
            throw th;
        } finally {
            visitorBook.cleanupVisitorBook();
            bootstrapContext.cleanupThreadLocalVertexExistenceSet();
            MultiTenantContext.setTenant(originalTenant);
        }
    }

    @Override
    public void visit(Play entity, //
            ParsedDependencies parsedDependencies) throws Exception {
        ImmutablePair<String, String> visitorEntry = new ImmutablePair<>(entity.getName(), VertexType.PLAY);
        if (!visitorBook.hasVisitEntry(visitorEntry)) {
            if (entity != null && entity.getDeleted()) {
                throw new RuntimeException(String.format(
                        "Cannot add PLAY %s to the graph as it is already deleted. "
                                + "System is already in inconsistent state. "
                                + "Please delete corresponding object before contibuing with bootstrap service.",
                        entity.getName()));

            }
            createPrereqVertices(parsedDependencies);
            cdlGraphAction.createPlayVertex(entity);
            visitorBook.addVisitEntry(visitorEntry);
        }
    }

    @Override
    public void visit(RatingEngine entity, //
            ParsedDependencies parsedDependencies) throws Exception {
        ImmutablePair<String, String> visitorEntry = new ImmutablePair<>(entity.getId(), VertexType.RATING_ENGINE);
        if (!visitorBook.hasVisitEntry(visitorEntry)) {
            if (entity != null && entity.getDeleted()) {
                throw new RuntimeException(String.format(
                        "Cannot add rating engine %s to the graph as it is already deleted. "
                                + "System is already in inconsistent state. "
                                + "Please delete corresponding object before contibuing with bootstrap service.",
                        entity.getId()));

            }
            createPrereqVertices(parsedDependencies);
            cdlGraphAction.createRatingEngineVertex(entity);
            visitorBook.addVisitEntry(visitorEntry);
        }
    }

    @Override
    public void visit(AIModel entity, //
            ParsedDependencies parsedDependencies) throws Exception {
        ImmutablePair<String, String> visitorEntry = new ImmutablePair<>(entity.getId(), "AIModel");
        if (!visitorBook.hasVisitEntry(visitorEntry)) {
            createPrereqVertices(parsedDependencies);
            cdlGraphAction.createEdgesForAIModel(entity, entity.getRatingEngine().getId());
            visitorBook.addVisitEntry(visitorEntry);
        }
    }

    @Override
    public void visit(RuleBasedModel entity, //
            ParsedDependencies parsedDependencies) throws Exception {
        ImmutablePair<String, String> visitorEntry = new ImmutablePair<>(entity.getId(), "RuleBasedModel");
        if (!visitorBook.hasVisitEntry(visitorEntry)) {
            createPrereqVertices(parsedDependencies);
            cdlGraphAction.createEdgesForRuleBasedModel(entity, entity.getRatingEngine().getId());
            visitorBook.addVisitEntry(visitorEntry);
        }
    }

    @Override
    public void visit(MetadataSegment entity, //
            ParsedDependencies parsedDependencies) throws Exception {
        ImmutablePair<String, String> visitorEntry = new ImmutablePair<>(entity.getName(), VertexType.SEGMENT);
        if (!visitorBook.hasVisitEntry(visitorEntry)) {
            createPrereqVertices(parsedDependencies);
            cdlGraphAction.createSegmentVertex(entity);
            visitorBook.addVisitEntry(visitorEntry);
        }
    }

    private void createTenantVertex() throws Exception {
        Tenant tenant = MultiTenantContext.getTenant();
        VertexCreationRequest request = new VertexCreationRequest();
        request.setObjectId(tenant.getId());
        request.setType(VertexType.TENANT);
        graphEntityManager.addVertex(tenant.getId(), null, null, null, request);
    }

    private void traversePlays(PlayEntityMgrImpl entityMgr) throws Exception {
        List<Play> plays = entityMgr.findAll();
        if (CollectionUtils.isNotEmpty(plays)) {
            plays.stream() //
                    .filter(pl -> pl.getDeleted() != Boolean.TRUE) //
                    .forEach(pl -> {
                        try {
                            entityMgr.accept(this, pl);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
    }

    private void traverseRatingEngines(RatingEngineEntityMgrImpl entityMgr) throws Exception {
        List<RatingEngine> ratingEngines = entityMgr.findAll();
        if (CollectionUtils.isNotEmpty(ratingEngines)) {
            ratingEngines.stream() //
                    .filter(re -> re.getDeleted() != Boolean.TRUE) //
                    .forEach(re -> {
                        try {
                            entityMgr.accept(this, re);
                            handleRatingModels(re.getId());
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
    }

    private void traverseSegments(SegmentEntityMgrImpl entityMgr) throws Exception {
        List<MetadataSegment> segments = entityMgr.findAll();
        if (CollectionUtils.isNotEmpty(segments)) {
            segments.stream() //
                    .forEach(seg -> {
                        try {
                            entityMgr.accept(this, seg);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
    }

    private void createPrereqVertices(ParsedDependencies parsedDependencies) {
        if (CollectionUtils.isNotEmpty(parsedDependencies.getAddDependencies())) {
            parsedDependencies.getAddDependencies().stream() //
                    .forEach(dep -> {
                        try {
                            traverse(dep);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
    }

    private void traverse(Triple<String, String, String> object) throws Exception {
        String objectId = object.getLeft();
        String objectType = object.getMiddle();
        switch (objectType) {
        case VertexType.PLAY:
            playEntityMgr.accept(this, playEntityMgr.getPlayByName(objectId, false));
            break;
        case VertexType.SEGMENT:
            segmentEntityMgr.accept(this, segmentEntityMgr.findByName(objectId));
            break;
        case VertexType.RATING_ENGINE:
            ratingEngineEntityMgr.accept(this, ratingEngineEntityMgr.findById(objectId));
            handleRatingModels(objectId);
            break;
        case VertexType.TENANT:
            createTenantVertex();
            break;
        case VertexType.RATING_ATTRIBUTE:
        case VertexType.RATING_EV_ATTRIBUTE:
        case VertexType.RATING_PROB_ATTRIBUTE:
        case VertexType.RATING_SCORE_ATTRIBUTE:
            Pair<ScoreType, String> ratingTypeNModelIdPair = ratingAttributeNameParser.parseTypeNMoelId(objectType,
                    objectId);
            String ratingEngineId = ratingTypeNModelIdPair.getRight();
            RatingEngine ratingEngine = ratingEngineEntityMgr.findById(ratingEngineId);
            if (ratingEngine == null) {
                throw new RuntimeException(String.format(
                        "Invalid ratingEngineId %s attribute %s has been used in segment or rule based rating engine, "
                                + "system is already in inconsistent state. Please delete corresponding object before contibuing with bootstrap service.",
                        ratingEngineId, objectId));
            }
            ratingEngineEntityMgr.accept(this, ratingEngine);
            handleRatingModels(ratingEngineId);
            break;
        default:
            log.error(String.format("Not yet implemented: %s", objectType));
        }
    }

    private void handleRatingModels(String ratingEngineId) {
        RatingEngine re = ratingEngineEntityMgr.findById(ratingEngineId);
        if (re.getType() == RatingEngineType.RULE_BASED) {
            List<RuleBasedModel> ruleModels = ruleBasedModelEntityMgrImpl.findAllByRatingEngineId(ratingEngineId);
            if (CollectionUtils.isNotEmpty(ruleModels)) {
                ruleModels.stream().forEach(rm -> {
                    try {
                        rm = ruleBasedModelEntityMgrImpl.findById(rm.getId());
                        ruleBasedModelEntityMgrImpl.accept(this, rm);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        } else {
            List<AIModel> aiModels = aiModelEntityMgrImpl.findAllByRatingEngineId(ratingEngineId);
            if (CollectionUtils.isNotEmpty(aiModels)) {
                aiModels.stream().forEach(am -> {
                    try {
                        am = aiModelEntityMgrImpl.findById(am.getId());
                        aiModelEntityMgrImpl.accept(this, am);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }
    }
}
