package com.latticeengines.cdl.workflow.steps.rating;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.graph.PrimitiveGraphNode;
import com.latticeengines.common.exposed.graph.StringGraphNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessRatingStepConfiguration;
import com.latticeengines.graph.util.DependencyGraphEngine;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("splitRatingEngines")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SplitRatingEngines extends BaseWorkflowStep<ProcessRatingStepConfiguration> {

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Value("${cdl.pa.default.max.iteration}")
    private int defaultMaxIteration;

    @Value("${cdl.processAnalyze.partial.rating.update.enabled}")
    private boolean partialUpdateEnabled;

    private String customerSpace;

    @Override
    public void execute() {
        customerSpace = configuration.getCustomerSpace().toString();
        List<RatingModelContainer> activeModels = getListObjectFromContext(RATING_MODELS, RatingModelContainer.class);

        List<String> impactedSegments = getListObjectFromContext(ACTION_IMPACTED_SEGMENTS, String.class);
        List<String> impactedEngines = getListObjectFromContext(ACTION_IMPACTED_ENGINES, String.class);
        List<List<RatingModelContainer>> generations = //
                splitIntoGenerations(activeModels, impactedSegments, impactedEngines);

        putObjectInContext(RATING_MODELS_BY_ITERATION, generations);
        putObjectInContext(CURRENT_RATING_ITERATION, 0);
    }

    private List<List<RatingModelContainer>> splitIntoGenerations(List<RatingModelContainer> containers, //
                                                                  List<String> impactedSegments, //
                                                                  List<String> impactedEngines) {
        List<List<RatingModelContainer>> iterations = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(containers)) {
            int maxIteration = configuration.getMaxIteration();
            if (maxIteration == 0) {
                maxIteration = defaultMaxIteration;
            }
            log.info("Max iteration is " + maxIteration + ".");
            if (maxIteration == 1) {
                log.info("Max Iteration is " + maxIteration + ", run single pass.");
                iterations = Collections.singletonList(containers);
            } else {
                try {
                    List<Set<String>> generations = //
                            getREDepGraphLayers(containers, impactedSegments, impactedEngines, maxIteration);
                    log.info("Got dependency graph layers: \n" + JsonUtils.pprint(generations));
                    if (CollectionUtils.isNotEmpty(generations) && generations.size() > 1) {
                        Map<String, RatingModelContainer> containerMap = new HashMap<>();
                        containers.forEach(
                                container -> containerMap.put(container.getEngineSummary().getId(), container));
                        for (Set<String> generation : generations) {
                            List<RatingModelContainer> iteration = generation.stream() //
                                    .map(containerMap::get).collect(Collectors.toList());
                            iterations.add(iteration);
                        }
                    } else if (CollectionUtils.isEmpty(generations)) {
                        log.info("No rating engines to score, all iterations are dummy.");
                        iterations = Collections.emptyList();
                    } else {
                        log.info("No more than 1 generation, run single pass.");
                        iterations = Collections.singletonList(containers);
                    }
                } catch (Exception e) {
                    log.warn("Failed to resolve dependency generations, just run single pass.", e);
                    iterations = Collections.singletonList(containers);
                }
            }
        }
        return iterations;
    }

    private List<Set<String>> getREDepGraphLayers(List<RatingModelContainer> containers, //
                                                  List<String> impactedSegments, //
                                                  List<String> impactedEngines, //
                                                  int maxIter) {
        try (DependencyGraphEngine graphEngine = new DependencyGraphEngine()) {
            Map<String, StringGraphNode> nodes = getREDepGraph(containers);
            graphEngine.loadGraphNodes(nodes.values());
            boolean rebuildAll = Boolean.TRUE.equals(getObjectFromContext(RESCORE_ALL_RATINGS, Boolean.class));
            List<Set<StringGraphNode>> layers;
            if (!rebuildAll && partialUpdateEnabled) {
                Collection<StringGraphNode> seed = //
                        getImpactedEngines(nodes, containers, impactedSegments, impactedEngines);
                log.info("Going to only update impacted engines: " + seed);
                layers = graphEngine.getDependencyLayersForSubDAG(StringGraphNode.class, maxIter, seed);
            } else {
                layers = graphEngine.getDependencyLayers(StringGraphNode.class, maxIter);
            }
            return layers.stream().map(set -> //
                    set.stream().map(PrimitiveGraphNode::getVal).collect(Collectors.toSet()) //
            ).collect(Collectors.toList());
        }
    }

    private Map<String, StringGraphNode> getREDepGraph(List<RatingModelContainer> containers) {
        final ConcurrentMap<String, StringGraphNode> nodes = new ConcurrentHashMap<>();
        final Set<String> engineIds = containers.stream() //
                .map(container -> {
                    String engineId = container.getEngineSummary().getId();
                    if (!nodes.containsKey(engineId)) {
                        nodes.putIfAbsent(engineId, new StringGraphNode(engineId));
                    }
                    return engineId;
                }).collect(Collectors.toSet());
        List<Runnable> runnables = new ArrayList<>();
        containers.forEach(container -> {
            Runnable runnable = () -> {
                String engineId = container.getEngineSummary().getId();
                RatingEngineType engineType = container.getEngineSummary().getType();
                String modelId = container.getModel().getId();
                List<AttributeLookup> attributeLookups = ratingEngineProxy.getDependingAttrsForModel(customerSpace,
                        engineType, modelId);
                if (CollectionUtils.isNotEmpty(attributeLookups)) {
                    attributeLookups.forEach(attributeLookup -> {
                        if (BusinessEntity.Rating.equals(attributeLookup.getEntity())) {
                            String dependingEngineId = RatingEngine.toEngineId(attributeLookup.getAttribute());
                            if (engineIds.contains(dependingEngineId)) {
                                StringGraphNode child = nodes.get(dependingEngineId);
                                nodes.get(engineId).addChild(child);
                            }
                        }
                    });
                }
                List<String> childIds = nodes.get(engineId).getChildren().stream().map(PrimitiveGraphNode::getVal)
                        .collect(Collectors.toList());
                log.info(String.format("Engine %s depends on %s", engineId, childIds));
            };
            runnables.add(runnable);
        });
        ExecutorService threadPool = ThreadPoolUtils.getFixedSizeThreadPool("dep-attrs", Math.min(8, engineIds.size()));
        ThreadPoolUtils.runRunnablesInParallel(threadPool, runnables, 60, 1);
        threadPool.shutdown();
        return nodes;
    }

    private Collection<StringGraphNode> getImpactedEngines(Map<String, StringGraphNode> nodes, //
                                                           List<RatingModelContainer> containers, List<String> impactedSegments, List<String> impactedEngines) {
        log.info(String.format("Engines impacted by actions: %s", StringUtils.join(impactedEngines, ", ")));
        for (RatingModelContainer container : containers) {
            String segmentName = container.getEngineSummary().getSegmentName();
            String engineId = container.getEngineSummary().getId();
            if (!impactedEngines.contains(engineId) && impactedSegments.contains(segmentName)) {
                impactedEngines.add(engineId);
                log.info(String.format("%s is impacted due to the change in %s", engineId, segmentName));
            }
        }
        return impactedEngines.stream().filter(nodes::containsKey).map(nodes::get).collect(Collectors.toList());
    }

}
