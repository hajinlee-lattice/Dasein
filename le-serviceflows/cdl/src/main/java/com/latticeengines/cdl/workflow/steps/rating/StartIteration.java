package com.latticeengines.cdl.workflow.steps.rating;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessRatingStepConfiguration;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("startIteration")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class StartIteration extends BaseWorkflowStep<ProcessRatingStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(StartIteration.class);

    @Override
    public void execute() {
        int iteration = getObjectFromContext(CURRENT_RATING_ITERATION, Integer.class) + 1;
        int maxIteration = configuration.getMaxIteration();
        putObjectInContext(CURRENT_RATING_ITERATION, iteration);
        log.info("Start rating iteration " + iteration + " / " + maxIteration + ".");

        removeObjectFromContext(AI_RAW_RATING_TABLE_NAME);
        removeObjectFromContext(RULE_RAW_RATING_TABLE_NAME);
        removeObjectFromContext(TABLES_GOING_TO_DYNAMO);
        removeObjectFromContext(TABLES_GOING_TO_REDSHIFT);
        removeObjectFromContext(STATS_TABLE_NAMES);

        removeObjectFromContext(ITERATION_RATING_MODELS);
        removeObjectFromContext(ITERATION_INACTIVE_ENGINES);

        List<RatingModelContainer> containers = getListObjectFromContext(RATING_MODELS, RatingModelContainer.class);
        List<String> inactiveEngines = getListObjectFromContext(INACTIVE_ENGINES, String.class);

        List<List> generations = getListObjectFromContext(RATING_MODELS_BY_ITERATION, List.class);
        if (iteration > CollectionUtils.size(generations)) {
            log.info("Current iteration " + iteration + " is larger than max dependency depth "
                    + CollectionUtils.size(generations) + ", skip iteration.");
        } else {
            List list = generations.get(iteration - 1);
            List<RatingModelContainer> containersInIteration = JsonUtils.convertList(list, RatingModelContainer.class);
            log.info("Found " + containersInIteration.size() + " active models to process in this iteration.");
            List<String> inactiveEnginesInIteration = new ArrayList<>();
            if (CollectionUtils.isNotEmpty(inactiveEngines)) {
                inactiveEnginesInIteration.addAll(inactiveEngines);
            }
            Set<String> activeEnginesInIteration = containersInIteration.stream()
                    .map(container -> container.getEngineSummary().getId()).collect(Collectors.toSet());
            containers.forEach(container -> {
                String engineId = container.getEngineSummary().getId();
                if (!activeEnginesInIteration.contains(engineId)) {
                    inactiveEnginesInIteration.add(engineId);
                }
            });

            log.info("Active engines for this iteration: " + new ArrayList<>(activeEnginesInIteration));
            log.info("Inactive engines for this iteration: " + inactiveEnginesInIteration);

            putObjectInContext(ITERATION_RATING_MODELS, containersInIteration);
            putObjectInContext(ITERATION_INACTIVE_ENGINES, inactiveEnginesInIteration);
        }
    }

}
