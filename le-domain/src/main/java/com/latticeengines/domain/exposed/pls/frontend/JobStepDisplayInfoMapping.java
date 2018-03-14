package com.latticeengines.domain.exposed.pls.frontend;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JobStepDisplayInfoMapping {
    private static final Logger LOGGER = LoggerFactory.getLogger(JobStepDisplayInfoMapping.class);

    private static final String[] FIT_MODEL_STEPS = { //
            "load_data", //
            "load_data", //
            "match_data", //
            "match_data", //
            "match_data", //
            "generate_insights", //
            "generate_insights", //
            "generate_insights", //
            "generate_insights", //
            "create_global_model", //
            "create_global_model", //
            "create_global_model", //
            "create_global_target_market", //
            "create_global_target_market", //
            "score_training_set", //
            "score_training_set", //
            "score_training_set" //
    };

    private static final String[] IMPORT_MATCH_AND_MODEL_STEPS = { //
            "load_data", //
            "load_data", //
            "generate_insights", //
            "generate_insights", //
            "generate_insights", //
            "generate_insights", //
            "generate_insights", //
            "generate_insights", //
            "generate_insights", //
            "create_global_target_market", //
            "create_global_target_market", //
            "create_global_target_market", //
            "create_global_target_market", //
            "create_global_target_market", //
            "create_global_target_market", //
            "create_global_target_market", //
            "create_global_target_market", //
            "create_global_target_market", //
            "create_global_target_market", //
            "create_global_target_market", //
            "create_global_target_market", //
            "create_global_target_market", //
            "create_global_target_market", //
            "create_global_target_market", //
            "create_global_target_market", //
            "score_training_set", //
            "score_training_set", //
            "score_training_set", //
            "score_training_set", //
            "score_training_set", //
            "score_training_set", //
            "score_training_set", //
            "score_training_set" //
    };

    private static final String[] RATING_IMPORT_MATCH_AND_MODEL_STEPS = { //
            "load_data", //
            "load_data", //
            "create_global_target_market", //
            "create_global_target_market", //
            "create_global_target_market", //
            "create_global_target_market", //
            "create_global_target_market", //
            "create_global_target_market", //
            "create_global_target_market", //
            "create_global_target_market", //
            "create_global_target_market", //
            "create_global_target_market", //
            "create_global_target_market", //
            "create_global_target_market", //
            "create_global_target_market", //
            "score_training_set", //
            "score_training_set", //
            "score_training_set", //
            "score_training_set", //
            "score_training_set", //
            "score_training_set", //
            "score_training_set", //
            "score_training_set", //
            "score_training_set", //
            "score_training_set", //
            "score_training_set", //
            "score_training_set", //
            "score_training_set", //
    };

    private static final String[] MODEL_AND_EMAIL_STEPS = { //
            "load_data", //
            "load_data", //
            "generate_insights", //
            "generate_insights", //
            "create_global_target_market", //
            "create_global_target_market", //
            "create_global_target_market", //
            "create_global_target_market", //
            "create_global_target_market", //
            "create_global_target_market", //
            "create_global_target_market", //
            "create_global_target_market", //
            "create_global_target_market", //
            "create_global_target_market", //
            "create_global_target_market", //
            "score_training_set", //
            "score_training_set", //
            "score_training_set", //
            "score_training_set", //
            "score_training_set" //
    };

    private static final String[] PMML_MODEL_STEPS = { //
            "create_global_target_market", //
            "download_model_summary" //
    };

    private static final String[] PLAY_LAUNCH_STEPS = { //
            "launch_play" //
    };

    private static final String[] EXPORT_SEGMENT_STEPS = { //
            "Segment Export" //
    };

    private static final String[] PROCESS_ANALYZE_STEPS = { //
            "Merging, De-duping & matching to Lattice Data Cloud", //
            "Merging, De-duping & matching to Lattice Data Cloud", //
            "Merging, De-duping & matching to Lattice Data Cloud", //
            "Merging, De-duping & matching to Lattice Data Cloud", //
            "Analyzing", //
            "Analyzing", //
            "Analyzing", //
            "Analyzing", //
            "Analyzing", //
            "Analyzing", //
            "Analyzing", //
            "Analyzing", //
            "Analyzing", //
            "Analyzing", //
            "Analyzing", //
            "Analyzing", //
            "Analyzing", //
            "Analyzing", //
            "Analyzing", //
            "Analyzing", //
            "Analyzing", //
            "Analyzing", //
            "Analyzing", //
            "Analyzing", //
            "Analyzing", //
            "Analyzing", //
            "Analyzing", //
            "Analyzing", //
            "Analyzing", //
            "Analyzing", //
            "Analyzing", //
            "Analyzing", //
            "Analyzing", //
            "Analyzing", //
            "Analyzing", //
            "Analyzing", //
            "Analyzing", //
            "Analyzing", //
            "Analyzing", //
            "Analyzing", //
            "Analyzing", //
            "Analyzing", //
            "Analyzing", //
            "Publishing", //
            "Publishing", //
            "Scoring", //
            "Scoring", //
            "Scoring", //
            "Scoring", //
            "Scoring", //
            "Scoring", //
            "Scoring", //
            "Scoring", //
            "Scoring", //
            "Scoring" //
    };

    private static final String[] RTS_BULK_SCORE_STEPS = {
            "scoring_targeted_accounts", //
            "scoring_targeted_accounts", //
            "scoring_targeted_accounts", //
            "scoring_targeted_accounts", //
            "scoring_targeted_accounts", //
            "scoring_targeted_accounts", //
            "scoring_targeted_accounts", //
            "scoring_targeted_accounts", //
            "scoring_targeted_accounts", //
            "scoring_targeted_accounts", //
    };

    private static final String[] CDL_DATA_FEED_IMPORT_STEPS = {
            "load_data", //
            "load_data" //
    };

    private static final Map<String, String[]> DISPLAY_NAME = new HashMap<>();
    private static final Map<String, String[]> DISPLAY_DESCRIPTION = new HashMap<>();
    static {
        DISPLAY_NAME.put("fitModelWorkflow", FIT_MODEL_STEPS);
        DISPLAY_NAME.put("importMatchAndModelWorkflow", IMPORT_MATCH_AND_MODEL_STEPS);
        DISPLAY_NAME.put("ratingEngineImportMatchAndModelWorkflow", RATING_IMPORT_MATCH_AND_MODEL_STEPS);
        DISPLAY_NAME.put("ratingEngineScoreWorkflow", RTS_BULK_SCORE_STEPS);
        DISPLAY_NAME.put("modelAndEmailWorkflow", MODEL_AND_EMAIL_STEPS);
        DISPLAY_NAME.put("pmmlModelWorkflow", PMML_MODEL_STEPS);
        DISPLAY_NAME.put("playLaunchWorkflow", PLAY_LAUNCH_STEPS);
        DISPLAY_NAME.put("segmentExportWorkflow", EXPORT_SEGMENT_STEPS);
        DISPLAY_NAME.put("processAnalyzeWorkflow", PROCESS_ANALYZE_STEPS);
        DISPLAY_NAME.put("rtsBulkScoreWorkflow", RTS_BULK_SCORE_STEPS);
        DISPLAY_NAME.put("cdlDataFeedImportWorkflow", CDL_DATA_FEED_IMPORT_STEPS);

        DISPLAY_DESCRIPTION.put("fitModelWorkflow", FIT_MODEL_STEPS);
        DISPLAY_DESCRIPTION.put("importMatchAndModelWorkflow", IMPORT_MATCH_AND_MODEL_STEPS);
        DISPLAY_DESCRIPTION.put("ratingEngineImportMatchAndModelWorkflow", RATING_IMPORT_MATCH_AND_MODEL_STEPS);
        DISPLAY_DESCRIPTION.put("ratingEngineScoreWorkflow", RTS_BULK_SCORE_STEPS);
        DISPLAY_DESCRIPTION.put("modelAndEmailWorkflow", MODEL_AND_EMAIL_STEPS);
        DISPLAY_DESCRIPTION.put("pmmlModelWorkflow", PMML_MODEL_STEPS);
        DISPLAY_DESCRIPTION.put("playLaunchWorkflow", PLAY_LAUNCH_STEPS);
        DISPLAY_DESCRIPTION.put("segmentExportWorkflow", EXPORT_SEGMENT_STEPS);
        DISPLAY_DESCRIPTION.put("processAnalyzeWorkflow", PROCESS_ANALYZE_STEPS);
        DISPLAY_DESCRIPTION.put("rtsBulkScoreWorkflow", RTS_BULK_SCORE_STEPS);
        DISPLAY_DESCRIPTION.put("cdlDataFeedImportWorkflow", CDL_DATA_FEED_IMPORT_STEPS);
    }

    public static String getMappedName(String workflowType, int stepIndex) {
        try {
            String[] steps = DISPLAY_NAME.get(workflowType);
            return steps[stepIndex];
        } catch (RuntimeException exc) {
            LOGGER.warn(
                    String.format("Have runtime error for workflow of type %s, at step %d.", workflowType, stepIndex),
                    exc);
            return "no_mapped_step_name";
        }
    }

    public static String getMappedDescription(String workflowType, int stepIndex) {
        try {
            String[] steps = DISPLAY_DESCRIPTION.get(workflowType);
            return steps[stepIndex];
        } catch (RuntimeException exc) {
            LOGGER.warn(
                    String.format("Have runtime error for workflow of type %s, at step %d.", workflowType, stepIndex),
                    exc);
            return "no_mapped_step_description";
        }
    }
}
