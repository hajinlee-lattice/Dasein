package com.latticeengines.cdl.workflow.steps.rating;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessRatingStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("startIteration")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class StartIteration extends BaseWorkflowStep<ProcessRatingStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(StartIteration.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Override
    public void execute() {
        int iteration = getObjectFromContext(CURRENT_RATING_ITERATION, Integer.class) + 1;
        putObjectInContext(CURRENT_RATING_ITERATION, iteration);
        log.info("Start rating iteration " + iteration + ".");

        removeObjectFromContext(AI_RAW_RATING_TABLE_NAME);
        removeObjectFromContext(RULE_RAW_RATING_TABLE_NAME);
        removeObjectFromContext(TABLES_GOING_TO_DYNAMO);
        removeObjectFromContext(TABLES_GOING_TO_REDSHIFT);
        removeObjectFromContext(STATS_TABLE_NAMES);

        removeObjectFromContext(ITERATION_RATING_MODELS);
        removeObjectFromContext(ITERATION_INACTIVE_ENGINES);

        List<RatingModelContainer> containers = getListObjectFromContext(RATING_MODELS, RatingModelContainer.class);
        List<String> inactiveEngines = getListObjectFromContext(INACTIVE_ENGINES, String.class);

        @SuppressWarnings("rawtypes")
        List<List> generations = getListObjectFromContext(RATING_MODELS_BY_ITERATION, List.class);
        if (iteration > CollectionUtils.size(generations)) {
            log.info("Current iteration " + iteration + " is larger than max dependency depth "
                    + CollectionUtils.size(generations) + ", skip iteration.");
        } else {
            String customerSpace = configuration.getCustomerSpace().toString();
            DataCollection.Version inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
            log.info("Evict attr repo cache for inactive version " + inactive);
            dataCollectionProxy.evictAttrRepoCache(customerSpace, inactive);

            @SuppressWarnings("rawtypes")
            List list = generations.get(iteration - 1);
            List<RatingModelContainer> containersInIteration = JsonUtils.convertList(list, RatingModelContainer.class);
            log.info("Found " + containersInIteration.size() + " active models to process in this iteration.");
            List<String> inactiveEnginesInIteration = new ArrayList<>();
            if (CollectionUtils.isNotEmpty(inactiveEngines)) {
                inactiveEnginesInIteration.addAll(inactiveEngines);
            }
            Set<String> activeEnginesInIteration = containersInIteration.stream()
                    .map(container -> container.getEngineSummary().getId()).collect(Collectors.toSet());
            for (RatingModelContainer container : containers) {
                String engineId = container.getEngineSummary().getId();
                if (!activeEnginesInIteration.contains(engineId)) {
                    inactiveEnginesInIteration.add(engineId);
                }
            }

            if (CollectionUtils.isNotEmpty(inactiveEnginesInIteration)) {
                Set<String> existingEngineIds = new HashSet<>();
                Table existingRatingTable;
                String previousIterationResultName = getStringValueFromContext(RATING_ITERATION_RESULT_TABLE_NAME);
                if (StringUtils.isNotBlank(previousIterationResultName)) {
                    existingRatingTable = metadataProxy.getTableSummary(customerSpace, previousIterationResultName);
                } else {
                    existingRatingTable = dataCollectionProxy.getTable(customerSpace, //
                            BusinessEntity.Rating.getServingStore(), inactive);
                }
                if (existingRatingTable != null) {
                    existingRatingTable.getAttributes().forEach(attr -> {
                        if (attr.getName().startsWith(RatingEngine.RATING_ENGINE_PREFIX)) {
                            String engineId = RatingEngine.toEngineId(attr.getName());
                            existingEngineIds.add(engineId);
                        }
                    });
                } else {
                    log.warn("There is no existing rating table, ignore all possible inactive engine ids.");
                }
                log.info("Existing engines in serving store for this iteration: " + existingEngineIds);
                inactiveEnginesInIteration = inactiveEnginesInIteration.stream() //
                        .filter(existingEngineIds::contains) //
                        .collect(Collectors.toList());
            }

            log.info("Active engines for this iteration: " + new ArrayList<>(activeEnginesInIteration));
            log.info("Inactive engines for this iteration: " + inactiveEnginesInIteration);

            putObjectInContext(ITERATION_RATING_MODELS, containersInIteration);
            putObjectInContext(ITERATION_INACTIVE_ENGINES, inactiveEnginesInIteration);
        }
    }

}
