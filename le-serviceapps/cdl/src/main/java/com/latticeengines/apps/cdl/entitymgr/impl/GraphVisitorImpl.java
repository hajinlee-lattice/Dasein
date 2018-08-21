package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
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
import com.latticeengines.graph.entity.GraphEntityManager;

@Component
public class GraphVisitorImpl implements GraphVisitor {

    @Inject
    private GraphEntityManager graphEntityManager;

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

    @Override
    public void visit(Play entity, //
            ParsedDependencies parsedDependencies) throws Exception {
        createPrereqVertices(parsedDependencies);
        cdlGraphAction.createPlayVertex(entity);
    }

    @Override
    public void visit(RatingEngine entity, //
            ParsedDependencies parsedDependencies) throws Exception {
        createPrereqVertices(parsedDependencies);
        cdlGraphAction.createRatingEngineVertex(entity);
    }

    @Override
    public void visit(AIModel entity, //
            ParsedDependencies parsedDependencies) throws Exception {
        createPrereqVertices(parsedDependencies);
        cdlGraphAction.createEdgesForAIModel(entity, entity.getRatingEngine().getId());
    }

    @Override
    public void visit(RuleBasedModel entity, //
            ParsedDependencies parsedDependencies) throws Exception {
        createPrereqVertices(parsedDependencies);
        cdlGraphAction.createEdgesForRuleBasedModel(entity, entity.getRatingEngine().getId());
    }

    @Override
    public void visit(MetadataSegment entity, //
            ParsedDependencies parsedDependencies) throws Exception {
        createPrereqVertices(parsedDependencies);
        cdlGraphAction.createSegmentVertex(entity);
    }

    public void traverse(Triple<String, String, String> object) throws Exception {
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
            ratingEngineEntityMgr.accept(this, ratingEngine);
            RatingEngine re = ratingEngineEntityMgr.findById(ratingEngineId);
            if (re.getType() == RatingEngineType.RULE_BASED) {
                List<RuleBasedModel> ruleModels = ruleBasedModelEntityMgrImpl.findAllByRatingEngineId(ratingEngineId);
                if (CollectionUtils.isNotEmpty(ruleModels)) {
                    ruleModels.stream().forEach(rm -> {
                        try {
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
                            aiModelEntityMgrImpl.accept(this, am);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
                }
            }
            break;
        default:
            throw new RuntimeException("Not yet implemented: " + objectType);
        }
    }

    @Override
    public void populateTenantGraph(Tenant tenant) throws Exception {
        Tenant originalTenant = MultiTenantContext.getTenant();
        try {
            MultiTenantContext.setTenant(tenant);
            createTenantVertex();
            traversePlays(playEntityMgr);
            traverseRatingEngines(ratingEngineEntityMgr);
            traverseSegments(segmentEntityMgr);
        } finally {
            MultiTenantContext.setTenant(originalTenant);
        }
    }

    public void createTenantVertex() throws Exception {
        Tenant tenant = MultiTenantContext.getTenant();
        VertexCreationRequest request = new VertexCreationRequest();
        request.setObjectId(tenant.getId());
        request.setType(VertexType.TENANT);
        graphEntityManager.addVertex(tenant.getId(), null, null, null, request);
    }

    public void traversePlays(PlayEntityMgrImpl entityMgr) throws Exception {
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

    public void traverseRatingEngines(RatingEngineEntityMgrImpl entityMgr) throws Exception {
        List<RatingEngine> ratingEngines = entityMgr.findAll();
        if (CollectionUtils.isNotEmpty(ratingEngines)) {
            ratingEngines.stream() //
                    .filter(re -> re.getDeleted() != Boolean.TRUE) //
                    .forEach(re -> {
                        try {
                            entityMgr.accept(this, re);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
    }

    public void traverseSegments(SegmentEntityMgrImpl entityMgr) throws Exception {
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
}
