package com.latticeengines.apps.cdl.entitymgr.impl;

import javax.inject.Inject;

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
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.graph.ParsedDependencies;
import com.latticeengines.domain.exposed.graph.VertexType;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;

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
    private CDLDependenciesToGraphAction cdlGraphAction;

    @Before("execution(* com.latticeengines.apps.cdl.entitymgr.impl.SegmentEntityMgrImpl.createSegment(..)) " +
            "|| execution(* com.latticeengines.apps.cdl.entitymgr.impl.SegmentEntityMgrImpl.createListSegment(..))")
    public void createSegment(JoinPoint joinPoint) throws Exception {
        if (joinPoint.getArgs().length > 0 && joinPoint.getArgs()[0] instanceof MetadataSegment) {
            MetadataSegment metadataSegment = (MetadataSegment) joinPoint.getArgs()[0];
            cdlGraphAction.createSegmentVertex(metadataSegment);
        }
    }

    @Around("execution(* com.latticeengines.apps.cdl.entitymgr.impl.SegmentEntityMgrImpl.delete*(..))")
    public void deleteSegment(ProceedingJoinPoint joinPoint) throws Throwable {
        if (joinPoint.getArgs().length > 1 && joinPoint.getArgs()[0] instanceof MetadataSegment) {
            MetadataSegment metadataSegment = (MetadataSegment) joinPoint.getArgs()[0];
            Boolean ignoreDependencyCheck = (Boolean) joinPoint.getArgs()[1];
            String segmentId = metadataSegment.getName();

            if (!ignoreDependencyCheck) {
                cdlGraphAction.checkDeleteSafety(segmentId, VertexType.SEGMENT);
            }
            joinPoint.proceed(joinPoint.getArgs());
            cdlGraphAction.deleteVertex(MultiTenantContext.getTenant().getId(), segmentId, VertexType.SEGMENT);
        }
    }

    @Around("execution(* com.latticeengines.apps.cdl.entitymgr.impl.SegmentEntityMgrImpl.updateSegment(..)) " +
            "|| execution(* com.latticeengines.apps.cdl.entitymgr.impl.SegmentEntityMgrImpl.updateListSegment(..))")
    public MetadataSegment updateSegment(ProceedingJoinPoint joinPoint) throws Throwable {
        if (joinPoint.getArgs().length > 1 && joinPoint.getArgs()[0] instanceof MetadataSegment
                && joinPoint.getArgs()[1] instanceof MetadataSegment) {
            MetadataSegment segment = (MetadataSegment) joinPoint.getArgs()[0];
            MetadataSegment existingSegment = (MetadataSegment) joinPoint.getArgs()[1];
            ParsedDependencies parsedDependencies = segmentEntityMgr.parse(segment, existingSegment);

            cdlGraphAction.addEdges(parsedDependencies, segment.getName(), VertexType.SEGMENT);

            MetadataSegment result = (MetadataSegment) joinPoint.proceed(joinPoint.getArgs());

            cdlGraphAction.dropEdges(parsedDependencies, segment.getName(), VertexType.SEGMENT);

            return result;
        }
        return null;
    }

    @Before("execution(* com.latticeengines.apps.cdl.entitymgr.impl.RatingEngineEntityMgrImpl.createRatingEngine(..))")
    public void createRating(JoinPoint joinPoint) throws Exception {
        if (joinPoint.getArgs().length > 0 && joinPoint.getArgs()[0] instanceof RatingEngine) {
            RatingEngine ratingEngine = (RatingEngine) joinPoint.getArgs()[0];
            cdlGraphAction.createRatingEngineVertex(ratingEngine);
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
                cdlGraphAction.checkDeleteSafety(ratingEngine.getId(), VertexType.RATING_ENGINE);
            }
            ParsedDependencies parsedDependencies = null;
            if (!isDelete && !alreadyDeleted) {
                parsedDependencies = ratingEngineEntityMgr.parse(ratingEngine, existingRatingEngine);

                cdlGraphAction.addEdges(parsedDependencies, ratingEngine.getId(), VertexType.RATING_ENGINE);
            }

            RatingEngine result = (RatingEngine) joinPoint.proceed(joinPoint.getArgs());

            if (!isDelete && !alreadyDeleted) {
                cdlGraphAction.dropEdges(parsedDependencies, ratingEngine.getId(), VertexType.RATING_ENGINE);
            }
            if (isDelete && !alreadyDeleted) {
                cdlGraphAction.deleteRatingAttributeVertices(ratingEngine);
                cdlGraphAction.deleteVertex(MultiTenantContext.getTenant().getId(), ratingEngine.getId(),
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
            cdlGraphAction.createEdgesForRuleBasedModel(ruleBasedModel, ratingEngineId);
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

            cdlGraphAction.addEdges(parsedDependencies, ratingEngineId, VertexType.RATING_ENGINE);

            RuleBasedModel result = (RuleBasedModel) joinPoint.proceed(joinPoint.getArgs());

            cdlGraphAction.dropEdges(parsedDependencies, ratingEngineId, VertexType.RATING_ENGINE);

            return result;
        }
        return null;
    }

    @Before("execution(* com.latticeengines.apps.cdl.entitymgr.impl.AIModelEntityMgrImpl.createAIModel(..))")
    public void createAIModel(JoinPoint joinPoint) throws Exception {
        if (joinPoint.getArgs().length > 1 && joinPoint.getArgs()[0] instanceof AIModel) {
            AIModel aiModel = (AIModel) joinPoint.getArgs()[0];
            String ratingEngineId = (String) joinPoint.getArgs()[1];
            cdlGraphAction.createEdgesForAIModel(aiModel, ratingEngineId);
        }
    }

    @Around("execution(* com.latticeengines.apps.cdl.entitymgr.impl.AIModelEntityMgrImpl.updateAIModel(..))")
    public AIModel updateAIModel(ProceedingJoinPoint joinPoint) throws Throwable {
        if (joinPoint.getArgs().length > 2 && joinPoint.getArgs()[0] instanceof AIModel) {
            AIModel ruleBasedModel = (AIModel) joinPoint.getArgs()[0];
            AIModel existingRuleBasedModel = (AIModel) joinPoint.getArgs()[1];
            String ratingEngineId = (String) joinPoint.getArgs()[2];
            ParsedDependencies parsedDependencies = aiModelEntityMgr.parse(ruleBasedModel, existingRuleBasedModel);

            cdlGraphAction.addEdges(parsedDependencies, ratingEngineId, VertexType.RATING_ENGINE);

            AIModel result = (AIModel) joinPoint.proceed(joinPoint.getArgs());

            cdlGraphAction.dropEdges(parsedDependencies, ratingEngineId, VertexType.RATING_ENGINE);

            return result;
        }
        return null;
    }

    @Around("execution(* com.latticeengines.apps.cdl.entitymgr.impl.RatingEngineEntityMgrImpl.deleteById(..))")
    public void deleteRating(ProceedingJoinPoint joinPoint) throws Throwable {
        if (joinPoint.getArgs().length > 0 && joinPoint.getArgs()[0] instanceof String) {
            String ratingId = (String) joinPoint.getArgs()[0];
            cdlGraphAction.checkDeleteSafety(ratingId, VertexType.RATING_ENGINE);

            RatingEngine ratingEngine = ratingEngineEntityMgr.findById(ratingId);

            joinPoint.proceed(joinPoint.getArgs());
            cdlGraphAction.deleteRatingAttributeVertices(ratingEngine);
            cdlGraphAction.deleteVertex(MultiTenantContext.getTenant().getId(), ratingEngine.getId(),
                    VertexType.RATING_ENGINE);
        }
    }

    @Before("execution(* com.latticeengines.apps.cdl.entitymgr.impl.PlayEntityMgrImpl.createPlay(..))")
    public void createPlay(JoinPoint joinPoint) throws Exception {
        if (joinPoint.getArgs().length > 0 && joinPoint.getArgs()[0] instanceof Play) {
            Play play = (Play) joinPoint.getArgs()[0];
            cdlGraphAction.createPlayVertex(play);
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
            if (play.getTargetSegment() == null) {
                play.setTargetSegment(existingPlay.getTargetSegment());
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
                cdlGraphAction.checkDeleteSafety(play.getName(), VertexType.PLAY);
            }
            ParsedDependencies parsedDependencies = null;
            if (!isDelete && !alreadyDeleted) {
                parsedDependencies = playEntityMgr.parse(play, existingPlay);

                cdlGraphAction.addEdges(parsedDependencies, play.getName(), VertexType.PLAY);
            }

            Play result = (Play) joinPoint.proceed(joinPoint.getArgs());

            if (!isDelete && !alreadyDeleted) {
                cdlGraphAction.dropEdges(parsedDependencies, play.getName(), VertexType.PLAY);
            }
            if (isDelete && !alreadyDeleted) {
                cdlGraphAction.checkDeleteSafety(play.getName(), VertexType.PLAY);
                cdlGraphAction.deleteVertex(MultiTenantContext.getTenant().getId(), play.getName(), VertexType.PLAY);
            }

            return result;
        }
        return null;
    }

    @Around("execution(* com.latticeengines.apps.cdl.entitymgr.impl.PlayEntityMgrImpl.deleteByName(..))")
    public void deletePlay(ProceedingJoinPoint joinPoint) throws Throwable {
        if (joinPoint.getArgs().length > 0 && joinPoint.getArgs()[0] instanceof String) {
            String playId = (String) joinPoint.getArgs()[0];
            cdlGraphAction.checkDeleteSafety(playId, VertexType.PLAY);

            if (joinPoint != null) {
                joinPoint.proceed(joinPoint.getArgs());
            }

            cdlGraphAction.deleteVertex(MultiTenantContext.getTenant().getId(), playId, VertexType.PLAY);
        }
    }
}
