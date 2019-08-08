package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.AIModelEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.PlayEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.RatingEngineEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.RuleBasedModelEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.SegmentEntityMgr;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.graph.EdgeType;
import com.latticeengines.domain.exposed.graph.GraphConstants;
import com.latticeengines.domain.exposed.graph.ParsedDependencies;
import com.latticeengines.domain.exposed.graph.VertexType;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngine.ScoreType;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.graphdb.DependenciesToGraphAction;

@Component
public class CDLDependenciesToGraphAction extends DependenciesToGraphAction {

    @Inject
    private SegmentEntityMgr segmentEntityMgr;

    @Inject
    private RatingEngineEntityMgr ratingEngineEntityMgr;

    @Inject
    private PlayEntityMgr playEntityMgr;

    @Inject
    private RuleBasedModelEntityMgr ruleBasedModelEntityMgr;

    @Inject
    private AIModelEntityMgr aiModelEntityMgr;

    @Inject
    private IdToDisplayNameTranslator nameTranslator;

    public void createSegmentVertex(MetadataSegment metadataSegment) throws Exception {
        ParsedDependencies parsedDependencies = segmentEntityMgr.parse(metadataSegment, null);
        try {
            beginThreadLocalCluster();
            createVertex(MultiTenantContext.getTenant().getId(), parsedDependencies, metadataSegment.getName(),
                    VertexType.SEGMENT, null, null);
        } finally {
            closeThreadLocalCluster();
        }
    }

    public void createRatingEngineVertex(RatingEngine ratingEngine) throws Exception {
        ParsedDependencies parsedDependencies = ratingEngineEntityMgr.parse(ratingEngine, null);
        try {
            beginThreadLocalCluster();
            createVertex(MultiTenantContext.getTenant().getId(), parsedDependencies, ratingEngine.getId(),
                    VertexType.RATING_ENGINE);
            createRatingAttributeVertices(ratingEngine);
        } finally {
            closeThreadLocalCluster();
        }
    }

    private void createRatingAttributeVertices(RatingEngine ratingEngine) throws Exception {
        try {
            beginThreadLocalCluster();
            switch (ratingEngine.getType()) {
            case CROSS_SELL:
            case PROSPECTING:
                createRatingAttr(ratingEngine, VertexType.RATING_EV_ATTRIBUTE,
                        "_" + RatingEngine.SCORE_ATTR_SUFFIX.get(ScoreType.ExpectedRevenue));
                createRatingAttr(ratingEngine, VertexType.RATING_PREDICTED_REV_ATTRIBUTE,
                        "_" + RatingEngine.SCORE_ATTR_SUFFIX.get(ScoreType.PredictedRevenue));
            case CUSTOM_EVENT:
                createRatingAttr(ratingEngine, VertexType.RATING_SCORE_ATTRIBUTE,
                        "_" + RatingEngine.SCORE_ATTR_SUFFIX.get(ScoreType.Score));
                createRatingAttr(ratingEngine, VertexType.RATING_PROB_ATTRIBUTE,
                        "_" + RatingEngine.SCORE_ATTR_SUFFIX.get(ScoreType.Probability));
            default:
                createRatingAttr(ratingEngine, VertexType.RATING_ATTRIBUTE, "");
            }
        } finally {
            closeThreadLocalCluster();
        }
    }

    public void createEdgesForRuleBasedModel(RuleBasedModel ruleBasedModel, String ratingEngineId) throws Exception {
        ParsedDependencies parsedDependencies = ruleBasedModelEntityMgr.parse(ruleBasedModel, null);
        try {
            beginThreadLocalCluster();

            addEdges(parsedDependencies, ratingEngineId, VertexType.RATING_ENGINE);
        } finally {
            closeThreadLocalCluster();
        }
    }

    public void createEdgesForAIModel(AIModel aiModel, String ratingEngineId) throws Exception {
        ParsedDependencies parsedDependencies = aiModelEntityMgr.parse(aiModel, null);
        try {
            beginThreadLocalCluster();

            addEdges(parsedDependencies, ratingEngineId, VertexType.RATING_ENGINE);
        } finally {
            closeThreadLocalCluster();
        }
    }

    public void createPlayVertex(Play play) throws Exception {
        ParsedDependencies parsedDependencies = playEntityMgr.parse(play, null);
        try {
            beginThreadLocalCluster();
            createVertex(MultiTenantContext.getTenant().getId(), parsedDependencies, play.getName(), VertexType.PLAY);
        } finally {
            closeThreadLocalCluster();
        }
    }

    public void addEdges(ParsedDependencies parsedDependencies, String vertexId, String vertexType) throws Exception {
        if (CollectionUtils.isNotEmpty(parsedDependencies.getAddDependencies())) {
            try {
                beginThreadLocalCluster();
                Map<Pair<Pair<String, String>, Pair<String, String>>, List<List<Map<String, String>>>> potentialCircularDependencies //
                        = addEdges(MultiTenantContext.getTenant().getId(), //
                                parsedDependencies, vertexId, vertexType);

                if (MapUtils.isNotEmpty(potentialCircularDependencies)) {
                    StringBuilder sb = new StringBuilder();
                    potentialCircularDependencies.keySet().stream() //
                            .forEach(k -> {
                                String fromVertexId = k.getLeft().getLeft();
                                String fromVertexType = k.getLeft().getRight();
                                String toVertexId = k.getRight().getLeft();
                                String toVertexType = k.getRight().getRight();
                                List<List<Map<String, String>>> translatedPaths = nameTranslator
                                        .translatePaths(potentialCircularDependencies.get(k));
                                String v1 = String.format("%s '%s'", nameTranslator.translateType(fromVertexType), //
                                        nameTranslator.idToDisplayName( //
                                                fromVertexType, fromVertexId));
                                String v2 = String.format("%s '%s'", nameTranslator.translateType(toVertexType), //
                                        nameTranslator.idToDisplayName( //
                                                toVertexType, toVertexId));
                                sb.append(String.format(
                                        "%s cannot depend on %s as it will cause circular dependencies. "
                                                + "Dependency path from %s to %s already exist: \n", //
                                        v1, v2, v2, v1));
                                translatedPaths.stream() //
                                        .forEach(p -> {
                                            sb.append("Path [");
                                            AtomicInteger firstTime = new AtomicInteger(1);
                                            p.stream() //
                                                    .forEach(v -> {
                                                        sb.append(String.format("%s%s '%s'", //
                                                                firstTime.get() == 1 ? "" : " -> ", //
                                                                v.get(IdToDisplayNameTranslator.TYPE), //
                                                                v.get(IdToDisplayNameTranslator.DISPLAY_NAME)));
                                                        firstTime.incrementAndGet();
                                                    });
                                            sb.append("]\n");
                                        });
                            });

                    throw new LedpException(LedpCode.LEDP_40041, new String[] { sb.toString() });
                }
            } finally {
                closeThreadLocalCluster();
            }
        }
    }

    public void dropEdges(ParsedDependencies parsedDependencies, String vertexId, String vertexType) throws Exception {
        if (CollectionUtils.isNotEmpty(parsedDependencies.getRemoveDependencies())) {
            try {
                beginThreadLocalCluster();
                dropEdges(MultiTenantContext.getTenant().getId(), //
                        parsedDependencies, vertexId, vertexType);
            } finally {
                closeThreadLocalCluster();
            }
        }
    }

    @SuppressWarnings("unused")
    private void checkDeleteSafetyWithId(String vertexId, String vertexType) throws Exception {
        List<Map<String, String>> dependencies = //
                checkDirectDependencies(MultiTenantContext.getTenant().getId(), //
                        vertexId, vertexType);

        if (CollectionUtils.isNotEmpty(dependencies)) {
            Map<String, List<Map<String, String>>> translatedDependencies = nameTranslator.translate(dependencies);
            throw new LedpException(LedpCode.LEDP_40042, new String[] { JsonUtils.serialize(translatedDependencies) });
        }
    }

    private String createRatingAttr(RatingEngine ratingEngine, String vertexType, String suffix) throws Exception {
        Tenant tenant = MultiTenantContext.getTenant();

        String objectId = BusinessEntity.Rating + "." + ratingEngine.getId() + suffix;
        ParsedDependencies parsedDependencies = new ParsedDependencies();
        List<Triple<String, String, String>> addDependencies = new ArrayList<>();
        List<Map<String, String>> edgeProperties = new ArrayList<>();
        addDependencies.add(ParsedDependencies //
                .tuple(ratingEngine.getId(), VertexType.RATING_ENGINE, EdgeType.DEPENDS_ON_VIA_PA));
        Map<String, String> edgeProps = new HashMap<>();
        edgeProps.put(GraphConstants.BEHAVIOR_ON_DEP_CHECK_TRAVERSAL_KEY, GraphConstants.JUMP_DURING_DEP_CHECK);
        edgeProps.put(GraphConstants.BEHAVIOR_ON_DELETE_OF_IN_VERTEX_KEY, GraphConstants.CASCADE_ON_DELETE);
        edgeProperties.add(edgeProps);
        addDependencies.add(ParsedDependencies //
                .tuple(tenant.getId(), VertexType.TENANT, EdgeType.TENANT));
        // this one is needed for tenant edge
        edgeProperties.add(null);
        parsedDependencies.setAddDependencies(addDependencies);

        Map<String, String> vertexProperties = new HashMap<>();
        vertexProperties.put(GraphConstants.BEHAVIOR_ON_DELETE_OF_IN_VERTEX_KEY, //
                GraphConstants.CASCADE_ON_DELETE);
        vertexProperties.put(GraphConstants.BEHAVIOR_ON_DEP_CHECK_TRAVERSAL_KEY, //
                GraphConstants.JUMP_DURING_DEP_CHECK);
        try {
            beginThreadLocalCluster();

            createVertex(MultiTenantContext.getTenant().getId(), parsedDependencies, objectId, vertexType,
                    vertexProperties, edgeProperties);
        } finally {
            closeThreadLocalCluster();
        }
        return objectId;
    }

    public void checkDeleteSafety(String vertexId, String vertexType) throws Exception {
        List<Map<String, String>> dependencies = //
                checkDirectDependencies(MultiTenantContext.getTenant().getId(), //
                        vertexId, vertexType);

        if (CollectionUtils.isNotEmpty(dependencies)) {
            Map<String, List<Map<String, String>>> translatedDependenciesWithId = nameTranslator
                    .translate(dependencies);
            Map<String, List<String>> translatedDependencies = new HashMap<>();
            translatedDependenciesWithId.keySet().stream() //
                    .filter(type -> CollectionUtils.isNotEmpty(translatedDependenciesWithId.get(type))) //
                    .forEach(type -> {
                        translatedDependencies.put(type, new ArrayList<>());
                        translatedDependenciesWithId.get(type).stream().forEach(dep -> {
                            translatedDependencies.get(type) //
                                    .add(dep.get(IdToDisplayNameTranslator.DISPLAY_NAME));
                        });
                    });
            throw new LedpException(LedpCode.LEDP_40042, new String[] { JsonUtils.serialize(translatedDependencies) });
        }
    }

    public void deleteRatingAttributeVertices(RatingEngine ratingEngine) throws Throwable {
        try {
            beginThreadLocalCluster();
            switch (ratingEngine.getType()) {
            case CROSS_SELL:
            case PROSPECTING:
                deleteRatingAttr(ratingEngine, VertexType.RATING_EV_ATTRIBUTE,
                        "_" + RatingEngine.SCORE_ATTR_SUFFIX.get(ScoreType.ExpectedRevenue));
                deleteRatingAttr(ratingEngine, VertexType.RATING_PREDICTED_REV_ATTRIBUTE,
                        "_" + RatingEngine.SCORE_ATTR_SUFFIX.get(ScoreType.PredictedRevenue));
            case CUSTOM_EVENT:
                deleteRatingAttr(ratingEngine, VertexType.RATING_SCORE_ATTRIBUTE,
                        "_" + RatingEngine.SCORE_ATTR_SUFFIX.get(ScoreType.Score));
                deleteRatingAttr(ratingEngine, VertexType.RATING_PROB_ATTRIBUTE,
                        "_" + RatingEngine.SCORE_ATTR_SUFFIX.get(ScoreType.Probability));
            default:
                deleteRatingAttr(ratingEngine, VertexType.RATING_ATTRIBUTE, "");
            }
        } finally {
            closeThreadLocalCluster();
        }
    }

    public void deleteRatingAttr(RatingEngine ratingEngine, String vertexType, String suffix) throws Throwable {
        String objectId = BusinessEntity.Rating + "." + ratingEngine.getId() + suffix;
        try {
            beginThreadLocalCluster();
            checkDeleteSafety(objectId, vertexType);
            deleteVertex(MultiTenantContext.getTenant().getId(), objectId, vertexType);
        } finally {
            closeThreadLocalCluster();
        }
    }
}
