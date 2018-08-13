package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;

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
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.graph.DependenciesToGraphAction;

@Aspect
public class DependencyGraphEntityMgrAspect {

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
    private DependenciesToGraphAction graphAction;

    @Inject
    private IdToDisplayNameTranslator nameTranslator;

    @Before("execution(* com.latticeengines.apps.cdl.entitymgr.impl.SegmentEntityMgrImpl.createSegment(..))")
    public void createSegment(JoinPoint joinPoint) throws Exception {
        if (joinPoint.getArgs().length > 0 && joinPoint.getArgs()[0] instanceof MetadataSegment) {
            MetadataSegment metadataSegment = (MetadataSegment) joinPoint.getArgs()[0];
            ParsedDependencies parsedDependencies = segmentEntityMgr.parse(metadataSegment, null);

            graphAction.createVertex(MultiTenantContext.getTenant().getId(), parsedDependencies,
                    metadataSegment.getName(), VertexType.SEGMENT, null, null);
        }
    }

    @Around("execution(* com.latticeengines.apps.cdl.entitymgr.impl.SegmentEntityMgrImpl.delete*(..))")
    public void deleteSegment(ProceedingJoinPoint joinPoint) throws Throwable {
        if (joinPoint.getArgs().length > 1 && joinPoint.getArgs()[0] instanceof MetadataSegment) {
            MetadataSegment metadataSegment = (MetadataSegment) joinPoint.getArgs()[0];
            Boolean ignoreDependencyCheck = (Boolean) joinPoint.getArgs()[1];
            String segmentId = metadataSegment.getName();

            if (!ignoreDependencyCheck) {
                checkDeleteSafety(segmentId, VertexType.SEGMENT);
            }
            joinPoint.proceed(joinPoint.getArgs());
            graphAction.deleteVertex(MultiTenantContext.getTenant().getId(), segmentId, VertexType.SEGMENT);
        }
    }

    @Around("execution(* com.latticeengines.apps.cdl.entitymgr.impl.SegmentEntityMgrImpl.updateSegment(..))")
    public MetadataSegment updateSegment(ProceedingJoinPoint joinPoint) throws Throwable {
        if (joinPoint.getArgs().length > 1 && joinPoint.getArgs()[0] instanceof MetadataSegment
                && joinPoint.getArgs()[1] instanceof MetadataSegment) {
            MetadataSegment segment = (MetadataSegment) joinPoint.getArgs()[0];
            MetadataSegment existingSegment = (MetadataSegment) joinPoint.getArgs()[1];
            ParsedDependencies parsedDependencies = segmentEntityMgr.parse(segment, existingSegment);

            addEdges(parsedDependencies, segment.getName(), VertexType.SEGMENT);

            MetadataSegment result = (MetadataSegment) joinPoint.proceed(joinPoint.getArgs());

            graphAction.dropEdges(MultiTenantContext.getTenant().getId(), parsedDependencies, segment.getName(),
                    VertexType.SEGMENT);

            return result;
        }
        return null;
    }

    @Before("execution(* com.latticeengines.apps.cdl.entitymgr.impl.RatingEngineEntityMgrImpl.createRatingEngine(..))")
    public void createRating(JoinPoint joinPoint) throws Exception {
        if (joinPoint.getArgs().length > 0 && joinPoint.getArgs()[0] instanceof RatingEngine) {
            RatingEngine ratingEngine = (RatingEngine) joinPoint.getArgs()[0];
            ParsedDependencies parsedDependencies = ratingEngineEntityMgr.parse(ratingEngine, null);
            graphAction.createVertex(MultiTenantContext.getTenant().getId(), parsedDependencies, ratingEngine.getId(),
                    VertexType.RATING_ENGINE);

            if (ratingEngine.getType() == RatingEngineType.RULE_BASED) {
                createRatingAttr(ratingEngine, VertexType.RATING_ATTRIBUTE, "");
            } else if (ratingEngine.getType() == RatingEngineType.CUSTOM_EVENT) {
                createRatingAttr(ratingEngine, VertexType.RATING_ATTRIBUTE, "");
                createRatingAttr(ratingEngine, VertexType.RATING_SCORE_ATTRIBUTE, "_score");
            } else {
                createRatingAttr(ratingEngine, VertexType.RATING_ATTRIBUTE, "");
                createRatingAttr(ratingEngine, VertexType.RATING_SCORE_ATTRIBUTE, "_score");
                createRatingAttr(ratingEngine, VertexType.RATING_EV_ATTRIBUTE, "_ev");
            }
        }
    }

    @Around("execution(* com.latticeengines.apps.cdl.entitymgr.impl.RatingEngineEntityMgrImpl.updateRatingEngine(..))")
    public RatingEngine updateRating(ProceedingJoinPoint joinPoint) throws Throwable {
        if (joinPoint.getArgs().length > 2 && joinPoint.getArgs()[0] instanceof RatingEngine
                && joinPoint.getArgs()[1] instanceof RatingEngine) {
            RatingEngine ratingEngine = (RatingEngine) joinPoint.getArgs()[0];
            RatingEngine existingRatingEngine = (RatingEngine) joinPoint.getArgs()[1];
            Boolean unlinkSegment = (Boolean) joinPoint.getArgs()[2];

            if (unlinkSegment) {
                ratingEngine.setSegment(null);
            } else {
                if (ratingEngine.getSegment() == null) {
                    ratingEngine.setSegment(existingRatingEngine.getSegment());
                }
            }

            boolean isDelete = false;
            boolean alreadyDeleted = false;
            if (ratingEngine.getDeleted() == Boolean.TRUE) {
                isDelete = true;
            }
            if (existingRatingEngine.getDeleted() == Boolean.TRUE) {
                alreadyDeleted = true;
            }

            if (isDelete && !alreadyDeleted) {
                checkDeleteSafety(ratingEngine.getId(), VertexType.RATING_ENGINE);
            }
            ParsedDependencies parsedDependencies = null;
            if (!isDelete && !alreadyDeleted) {
                parsedDependencies = ratingEngineEntityMgr.parse(ratingEngine, existingRatingEngine);

                addEdges(parsedDependencies, ratingEngine.getId(), VertexType.RATING_ENGINE);
            }

            RatingEngine result = (RatingEngine) joinPoint.proceed(joinPoint.getArgs());

            if (!isDelete && !alreadyDeleted) {
                graphAction.dropEdges(MultiTenantContext.getTenant().getId(), parsedDependencies, ratingEngine.getId(),
                        VertexType.RATING_ENGINE);
            }
            if (isDelete && !alreadyDeleted) {
                if (ratingEngine.getType() == RatingEngineType.RULE_BASED) {
                    deleteRatingAttr(ratingEngine, VertexType.RATING_ATTRIBUTE, "");
                } else if (ratingEngine.getType() == RatingEngineType.CUSTOM_EVENT) {
                    deleteRatingAttr(ratingEngine, VertexType.RATING_ATTRIBUTE, "");
                    deleteRatingAttr(ratingEngine, VertexType.RATING_SCORE_ATTRIBUTE, "_score");
                } else {
                    deleteRatingAttr(ratingEngine, VertexType.RATING_ATTRIBUTE, "");
                    deleteRatingAttr(ratingEngine, VertexType.RATING_SCORE_ATTRIBUTE, "_score");
                    deleteRatingAttr(ratingEngine, VertexType.RATING_EV_ATTRIBUTE, "_ev");
                }
                graphAction.deleteVertex(MultiTenantContext.getTenant().getId(), ratingEngine.getId(),
                        VertexType.RATING_ENGINE);
            }

            return result;
        }
        return null;
    }

    @Before("execution(* com.latticeengines.apps.cdl.entitymgr.impl.RuleBasedModelEntityMgrImpl.createRuleBasedModel(..))")
    public void createRuleBasedModel(JoinPoint joinPoint) throws Exception {
        if (joinPoint.getArgs().length > 1 && joinPoint.getArgs()[0] instanceof RuleBasedModel) {
            RuleBasedModel ruleBasedModel = (RuleBasedModel) joinPoint.getArgs()[0];
            String ratingEngineId = (String) joinPoint.getArgs()[1];
            ParsedDependencies parsedDependencies = ruleBasedModelEntityMgr.parse(ruleBasedModel, null);

            addEdges(parsedDependencies, ratingEngineId, VertexType.RATING_ENGINE);
        }
    }

    @Around("execution(* com.latticeengines.apps.cdl.entitymgr.impl.RuleBasedModelEntityMgrImpl.updateRuleBasedModel(..))")
    public RuleBasedModel updateRuleBasedModel(ProceedingJoinPoint joinPoint) throws Throwable {
        if (joinPoint.getArgs().length > 2 && joinPoint.getArgs()[0] instanceof RuleBasedModel) {
            RuleBasedModel ruleBasedModel = (RuleBasedModel) joinPoint.getArgs()[0];
            RuleBasedModel existingRuleBasedModel = (RuleBasedModel) joinPoint.getArgs()[1];
            String ratingEngineId = (String) joinPoint.getArgs()[2];
            ParsedDependencies parsedDependencies = ruleBasedModelEntityMgr.parse(ruleBasedModel,
                    existingRuleBasedModel);

            addEdges(parsedDependencies, ratingEngineId, VertexType.RATING_ENGINE);

            RuleBasedModel result = (RuleBasedModel) joinPoint.proceed(joinPoint.getArgs());

            graphAction.dropEdges(MultiTenantContext.getTenant().getId(), parsedDependencies, ratingEngineId,
                    VertexType.RATING_ENGINE);

            return result;
        }
        return null;
    }

    @Before("execution(* com.latticeengines.apps.cdl.entitymgr.impl.AIModelEntityMgrImpl.createAIModel(..))")
    public void createAIModel(JoinPoint joinPoint) throws Exception {
        if (joinPoint.getArgs().length > 1 && joinPoint.getArgs()[0] instanceof AIModel) {
            AIModel aiModel = (AIModel) joinPoint.getArgs()[0];
            String ratingEngineId = (String) joinPoint.getArgs()[1];
            ParsedDependencies parsedDependencies = aiModelEntityMgr.parse(aiModel, null);

            addEdges(parsedDependencies, ratingEngineId, VertexType.RATING_ENGINE);
        }
    }

    @Around("execution(* com.latticeengines.apps.cdl.entitymgr.impl.AIModelEntityMgrImpl.updateAIModel(..))")
    public AIModel updateAIModel(ProceedingJoinPoint joinPoint) throws Throwable {
        if (joinPoint.getArgs().length > 2 && joinPoint.getArgs()[0] instanceof AIModel) {
            AIModel ruleBasedModel = (AIModel) joinPoint.getArgs()[0];
            AIModel existingRuleBasedModel = (AIModel) joinPoint.getArgs()[1];
            String ratingEngineId = (String) joinPoint.getArgs()[2];
            ParsedDependencies parsedDependencies = aiModelEntityMgr.parse(ruleBasedModel, existingRuleBasedModel);

            addEdges(parsedDependencies, ratingEngineId, VertexType.RATING_ENGINE);

            AIModel result = (AIModel) joinPoint.proceed(joinPoint.getArgs());

            graphAction.dropEdges(MultiTenantContext.getTenant().getId(), parsedDependencies, ratingEngineId,
                    VertexType.RATING_ENGINE);

            return result;
        }
        return null;
    }

    @Around("execution(* com.latticeengines.apps.cdl.entitymgr.impl.RatingEngineEntityMgrImpl.deleteById(..))")
    public void deleteRating(ProceedingJoinPoint joinPoint) throws Throwable {
        if (joinPoint.getArgs().length > 0 && joinPoint.getArgs()[0] instanceof String) {
            String ratingId = (String) joinPoint.getArgs()[0];
            checkDeleteSafety(ratingId, VertexType.RATING_ENGINE);

            RatingEngine ratingEngine = ratingEngineEntityMgr.findById(ratingId);

            joinPoint.proceed(joinPoint.getArgs());

            if (ratingEngine.getType() == RatingEngineType.RULE_BASED) {
                deleteRatingAttr(ratingEngine, VertexType.RATING_ATTRIBUTE, "");
            } else if (ratingEngine.getType() == RatingEngineType.CUSTOM_EVENT) {
                deleteRatingAttr(ratingEngine, VertexType.RATING_ATTRIBUTE, "");
                deleteRatingAttr(ratingEngine, VertexType.RATING_SCORE_ATTRIBUTE, "_score");
            } else {
                deleteRatingAttr(ratingEngine, VertexType.RATING_ATTRIBUTE, "");
                deleteRatingAttr(ratingEngine, VertexType.RATING_SCORE_ATTRIBUTE, "_score");
                deleteRatingAttr(ratingEngine, VertexType.RATING_EV_ATTRIBUTE, "_ev");
            }
            graphAction.deleteVertex(MultiTenantContext.getTenant().getId(), ratingEngine.getId(),
                    VertexType.RATING_ENGINE);
        }
    }

    @Before("execution(* com.latticeengines.apps.cdl.entitymgr.impl.PlayEntityMgrImpl.createPlay(..))")
    public void createPlay(JoinPoint joinPoint) throws Exception {
        if (joinPoint.getArgs().length > 0 && joinPoint.getArgs()[0] instanceof Play) {
            Play play = (Play) joinPoint.getArgs()[0];
            ParsedDependencies parsedDependencies = playEntityMgr.parse(play, null);
            graphAction.createVertex(MultiTenantContext.getTenant().getId(), parsedDependencies, play.getName(),
                    VertexType.PLAY);
        }
    }

    @Around("execution(* com.latticeengines.apps.cdl.entitymgr.impl.PlayEntityMgrImpl.updatePlay(..))")
    public Play updatePlay(ProceedingJoinPoint joinPoint) throws Throwable {
        if (joinPoint.getArgs().length > 1 && joinPoint.getArgs()[0] instanceof Play
                && joinPoint.getArgs()[1] instanceof Play) {
            Play play = (Play) joinPoint.getArgs()[0];
            Play existingPlay = (Play) joinPoint.getArgs()[1];

            if (play.getRatingEngine() == null) {
                play.setRatingEngine(existingPlay.getRatingEngine());
            }

            boolean isDelete = false;
            boolean alreadyDeleted = false;
            if (play.getDeleted() == Boolean.TRUE) {
                isDelete = true;
            }
            if (existingPlay.getDeleted() == Boolean.TRUE) {
                alreadyDeleted = true;
            }
            if (isDelete && !alreadyDeleted) {
                checkDeleteSafety(play.getName(), VertexType.PLAY);
            }
            ParsedDependencies parsedDependencies = null;
            if (!isDelete && !alreadyDeleted) {
                parsedDependencies = playEntityMgr.parse(play, existingPlay);

                addEdges(parsedDependencies, play.getName(), VertexType.PLAY);
            }

            Play result = (Play) joinPoint.proceed(joinPoint.getArgs());

            if (!isDelete && !alreadyDeleted) {
                graphAction.dropEdges(MultiTenantContext.getTenant().getId(), parsedDependencies, play.getName(),
                        VertexType.PLAY);
            }
            if (isDelete && !alreadyDeleted) {
                checkDeleteSafety(play.getName(), VertexType.PLAY);
                graphAction.deleteVertex(MultiTenantContext.getTenant().getId(), play.getName(), VertexType.PLAY);
            }

            return result;
        }
        return null;
    }

    @Around("execution(* com.latticeengines.apps.cdl.entitymgr.impl.PlayEntityMgrImpl.deleteByName(..))")
    public void deletePlay(ProceedingJoinPoint joinPoint) throws Throwable {
        if (joinPoint.getArgs().length > 0 && joinPoint.getArgs()[0] instanceof String) {
            String playId = (String) joinPoint.getArgs()[0];
            checkDeleteSafety(playId, VertexType.PLAY);

            if (joinPoint != null) {
                joinPoint.proceed(joinPoint.getArgs());
            }

            graphAction.deleteVertex(MultiTenantContext.getTenant().getId(), playId, VertexType.PLAY);
        }
    }

    public String createRatingAttr(RatingEngine ratingEngine, String vertexType, String suffix) throws Exception {
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
        edgeProperties.add(null);
        parsedDependencies.setAddDependencies(addDependencies);

        graphAction.createVertex(MultiTenantContext.getTenant().getId(), parsedDependencies, objectId, vertexType, null,
                edgeProperties);
        return objectId;
    }

    public void deleteRatingAttr(RatingEngine ratingEngine, String vertexType, String suffix) throws Throwable {
        String objectId = BusinessEntity.Rating + "." + ratingEngine.getId() + suffix;
        checkDeleteSafety(objectId, vertexType);
        graphAction.deleteVertex(MultiTenantContext.getTenant().getId(), objectId, vertexType);
    }

    private void checkDeleteSafety(String vertexId, String vertexType) throws Exception {
        List<Map<String, String>> dependencies = graphAction
                .checkDirectDependencies(MultiTenantContext.getTenant().getId(), vertexId, vertexType);

        if (CollectionUtils.isNotEmpty(dependencies)) {
            Map<String, List<Map<String, String>>> translatedDependencies = nameTranslator.translate(dependencies);
            throw new LedpException(LedpCode.LEDP_40042,
                    new String[] { vertexId, JsonUtils.serialize(translatedDependencies) });
        }
    }

    private void addEdges(ParsedDependencies parsedDependencies, String vertexId, String vertexType) throws Exception {
        Map<Pair<Pair<String, String>, Pair<String, String>>, List<List<Map<String, String>>>> potentialCircularDependencies = graphAction
                .addEdges(MultiTenantContext.getTenant().getId(), parsedDependencies, vertexId, vertexType);
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
                                nameTranslator.idToDisplayName(fromVertexId, //
                                        nameTranslator.translateType(fromVertexType)));
                        String v2 = String.format("%s '%s'", nameTranslator.translateType(toVertexType), //
                                nameTranslator.idToDisplayName(toVertexId, //
                                        nameTranslator.translateType(toVertexType)));
                        sb.append(String.format(
                                "%s cannot depend on %s as it will cause circular dependencies. "
                                        + "Dependency path from %s to %s already exist: ", //
                                v1, v2, v2, v1));
                        translatedPaths.stream() //
                                .forEach(p -> {
                                    sb.append("Path [");
                                    p.stream() //
                                            .forEach(v -> {
                                                sb.append(String.format("%s '%s' -> ",
                                                        v.get(IdToDisplayNameTranslator.TYPE),
                                                        v.get(IdToDisplayNameTranslator.DISPLAY_NAME)));
                                            });
                                    sb.append("]. ");
                                });
                    });

            throw new LedpException(LedpCode.LEDP_40041, new String[] { sb.toString() });
        }
    }
}
