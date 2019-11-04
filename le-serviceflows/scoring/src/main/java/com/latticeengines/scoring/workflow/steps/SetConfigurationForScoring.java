package com.latticeengines.scoring.workflow.steps;

import java.util.Collections;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.util.FeatureImportanceUtil;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.CustomEventModelingType;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.SetConfigurationForScoringConfiguration;
import com.latticeengines.domain.exposed.util.BucketMetadataUtils;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("setConfigurationForScoring")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SetConfigurationForScoring extends BaseWorkflowStep<SetConfigurationForScoringConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(SetConfigurationForScoring.class);

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Inject
    private FeatureImportanceUtil featureImportanceUtil;

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    public SetConfigurationForScoring() {
    }

    @Override
    public void execute() {
        log.info("Setting the configuration for scoring.");

        // for export score training file step in score workflow
        String sourceFileName = configuration.getInputProperties()
                .get(WorkflowContextConstants.Inputs.SOURCE_DISPLAY_NAME);
        String targetFileName = String.format("%s_scored_%s",
                StringUtils.substringBeforeLast(sourceFileName.replaceAll("[^A-Za-z0-9_]", "_"), ".csv"),
                DateTime.now().getMillis());
        Table eventTable = getObjectFromContext(EVENT_TABLE, Table.class);
        String outputPath = String.format("%s/%s/data/%s/csv_files/score_event_table_output/%s",
                configuration.getModelingServiceHdfsBaseDir(), configuration.getCustomerSpace(), eventTable.getName(),
                targetFileName);
        putStringValueInContext(EXPORT_SCORE_TRAINING_FILE_OUTPUT_PATH, outputPath);
        saveOutputValue(WorkflowContextConstants.Outputs.EXPORT_OUTPUT_PATH,
                getStringValueFromContext(EXPORT_SCORE_TRAINING_FILE_OUTPUT_PATH));

        setRatingModelsContext();
    }

    private void setRatingModelsContext() {
        if (CustomEventModelingType.LPI.equals(configuration.getCustomEventModelingType())) {
            return;
        }
        String engineId = configuration.getInputProperties().get(WorkflowContextConstants.Inputs.RATING_ENGINE_ID);
        if (StringUtils.isNotBlank(engineId)) {
            String modelId = configuration.getInputProperties().get(WorkflowContextConstants.Inputs.RATING_MODEL_ID);
            log.info("Constructing rating model container for RatingEngineId=" + engineId + " and RatingModelId="
                    + modelId);
            String customerSpace = configuration.getCustomerSpace().toString();
            RatingEngine ratingEngine = ratingEngineProxy.getRatingEngine(customerSpace, engineId);

            if (RatingEngineType.CROSS_SELL.equals(ratingEngine.getType())
                    || RatingEngineType.CUSTOM_EVENT.equals(ratingEngine.getType())) {
                AIModel ratingModel = (AIModel) ratingEngineProxy.getRatingModel(customerSpace, engineId, modelId);
                String modelGuid = ratingModel.getModelSummaryId();
                if (StringUtils.isBlank(modelGuid)) {
                    throw new RuntimeException("Must provide either model guid: " + JsonUtils.serialize(ratingModel));
                }

                RatingEngineSummary ratingEngineSummary = new RatingEngineSummary();
                ratingEngineSummary.setId(ratingEngine.getId());
                ratingEngineSummary.setDisplayName(ratingEngine.getDisplayName());
                ratingEngineSummary.setType(ratingEngine.getType());
                ratingEngineSummary.setStatus(ratingEngine.getStatus());
                ratingEngineSummary.setSegmentName(ratingEngine.getSegment().getName());
                ratingEngineSummary.setBucketMetadata(BucketMetadataUtils.getDefaultMetadata());

                RatingModelContainer container = new RatingModelContainer(ratingModel, ratingEngineSummary,
                        BucketMetadataUtils.getDefaultMetadata());
                putObjectInContext(ITERATION_RATING_MODELS, Collections.singletonList(container));

                ModelSummary modelSummary = modelSummaryProxy.findValidByModelId(customerSpace, modelId);
                putObjectInContext(SCORE_TRAINING_FILE_INCLUDED_FEATURES, featureImportanceUtil.getFeatureImportance(customerSpace, modelSummary).keySet());

                putStringValueInContext(SCORING_MODEL_ID, modelGuid);
            }
        }
    }

}
