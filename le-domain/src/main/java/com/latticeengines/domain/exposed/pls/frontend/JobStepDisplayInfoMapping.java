package com.latticeengines.domain.exposed.pls.frontend;

import java.util.HashMap;
import java.util.Map;

public final class JobStepDisplayInfoMapping {

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
            "score_training_set" //
    };

    private static final String[] CROSS_SELL_IMPORT_MATCH_AND_MODEL_STEPS = { //
            "load_data", //
            "load_data", //
            "match_data", //
            "match_data", //
            "match_data", //
            "match_data", //
            "match_data", //
            "match_data", //
            "match_data", //
            "match_data", //
            "match_data", //
            "match_data", //
            "match_data", //
            "match_data", //
            "match_data", //
            "match_data", //
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
            "score_training_set", //
    };

    private static final String[] MODEL_AND_EMAIL_STEPS = { //
            "load_data", //
            "load_data", //
            "load_data", //
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
            "score_training_set" };

    private static final String[] RATING_MODEL_AND_EMAIL_STEPS = { //
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
            "score_training_set" //
    };

    private static final String[] PMML_MODEL_STEPS = { //
            "create_global_target_market", //
            "download_model_summary" //
    };

    private static final String[] PLAY_LAUNCH_STEPS = { //
            "launch_play" //
    };

    private static final String[] SEGMENT_EXPORT_STEPS = { //
            "segment_export_init", //
            "export_data"
    };

    private static final String[] PROCESS_ANALYZE_STEPS = generatePAStepDescriptions();

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

    private static final String[] RATING_BULK_SCORE_STEPS = {
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
            "scoring_targeted_accounts", //
    };

    private static final String[] CDL_DATA_FEED_IMPORT_STEPS = {
            "load_data", //
            "load_data" //
    };

    private static final String[] IMPORT_MATCH_AND_SCORE_STEPS = {
            "load_data", //
            "load_data", //
            "load_data", //
            "load_data", //
            "load_data", //
            "load_data", //
            "load_data", //
            "load_data", //
            "load_data", //
            "load_data", //
            "match_data", //
            "match_data", //
            "match_data", //
            "match_data", //
            "match_data", //
            "match_data", //
            "match_data", //
            "match_data", //
            "match_data", //
            "match_data", //
            "score_data", //
            "score_data", //
            "score_data", //
            "score_data", //
            "score_data" };

    private static final String[] CUSTOM_EVENT_MODELING_STEPS = {
            "load_data", //
            "load_data", //
            "load_data", //
            "load_data", //
            "match_data", //
            "match_data", //
            "match_data", //
            "match_data", //
            "match_data", //
            "match_data", //
            "match_data", //
            "match_data", //
            "match_data", //
            "match_data", //
            "match_data", //
            "match_data", //
            "create_global_model", //
            "create_global_model", //
            "create_global_model", //
            "create_global_model", //
            "create_global_model", //
            "create_global_model", //
            "create_global_model", //
            "create_global_model", //
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
            "score_training_set" };

    private static final String[] CDL_OPERATION_STEPS = {
            "delete_file_upload", //
            "delete_file_upload", //
            "delete_file_upload", //
            "start_maintenance", //
            "start_maintenance", //
            "start_maintenance", //
            "cleanup_by_upload", //
            "cleanup_by_upload", //
            "cleanup_by_upload", //
            "operation_execute", //
            "operation_execute", //
            "operation_execute" };

    private static final String[] IMPORT_AND_RTS_BULK_SCORE_STEPS = {
            "import_data", //
            "import_data", //
            "create_table_import_report", //
            "create_table_import_report", //
            "match_data", //
            "match_data", //
            "match_data", //
            "match_data", //
            "score_training_set", //
            "score_training_set", //
            "score_training_set", //
            "score_training_set" };

    private static final String[] SCORE_STEPS = {
            "match_data", //
            "match_data", //
            "match_data", //
            "match_data", //
            "score_data", //
            "score_data", //
            "score_data", //
            "score_data" };

    private static final Map<String, String[]> DISPLAY_NAME = new HashMap<>();
    private static final Map<String, String[]> DISPLAY_DESCRIPTION = new HashMap<>();
    static {
        DISPLAY_NAME.put("fitModelWorkflow", FIT_MODEL_STEPS);
        DISPLAY_NAME.put("importMatchAndModelWorkflow", IMPORT_MATCH_AND_MODEL_STEPS);
        DISPLAY_NAME.put("crossSellImportMatchAndModelWorkflow",
                CROSS_SELL_IMPORT_MATCH_AND_MODEL_STEPS);
        DISPLAY_NAME.put("ratingEngineScoreWorkflow", RATING_BULK_SCORE_STEPS);
        DISPLAY_NAME.put("ratingEngineModelAndEmailWorkflow", RATING_MODEL_AND_EMAIL_STEPS);
        DISPLAY_NAME.put("modelAndEmailWorkflow", MODEL_AND_EMAIL_STEPS);
        DISPLAY_NAME.put("pmmlModelWorkflow", PMML_MODEL_STEPS);
        DISPLAY_NAME.put("playLaunchWorkflow", PLAY_LAUNCH_STEPS);
        DISPLAY_NAME.put("segmentExportWorkflow", SEGMENT_EXPORT_STEPS);
        DISPLAY_NAME.put("processAnalyzeWorkflow", PROCESS_ANALYZE_STEPS);
        DISPLAY_NAME.put("rtsBulkScoreWorkflow", RTS_BULK_SCORE_STEPS);
        DISPLAY_NAME.put("cdlDataFeedImportWorkflow", CDL_DATA_FEED_IMPORT_STEPS);
        DISPLAY_NAME.put("importMatchAndScoreWorkflow", IMPORT_MATCH_AND_SCORE_STEPS);
        DISPLAY_NAME.put("customEventModelingWorkflow", CUSTOM_EVENT_MODELING_STEPS);
        DISPLAY_NAME.put("cdlOperationWorkflow", CDL_OPERATION_STEPS);
        DISPLAY_NAME.put("importAndRTSBulkScoreWorkflow", IMPORT_AND_RTS_BULK_SCORE_STEPS);
        DISPLAY_NAME.put("scoreWorkflow", SCORE_STEPS);

        DISPLAY_DESCRIPTION.put("fitModelWorkflow", FIT_MODEL_STEPS);
        DISPLAY_DESCRIPTION.put("importMatchAndModelWorkflow", IMPORT_MATCH_AND_MODEL_STEPS);
        DISPLAY_DESCRIPTION.put("crossSellImportMatchAndModelWorkflow",
                CROSS_SELL_IMPORT_MATCH_AND_MODEL_STEPS);
        DISPLAY_DESCRIPTION.put("ratingEngineScoreWorkflow", RATING_BULK_SCORE_STEPS);
        DISPLAY_DESCRIPTION.put("ratingEngineModelAndEmailWorkflow", RATING_MODEL_AND_EMAIL_STEPS);
        DISPLAY_DESCRIPTION.put("modelAndEmailWorkflow", MODEL_AND_EMAIL_STEPS);
        DISPLAY_DESCRIPTION.put("pmmlModelWorkflow", PMML_MODEL_STEPS);
        DISPLAY_DESCRIPTION.put("playLaunchWorkflow", PLAY_LAUNCH_STEPS);
        DISPLAY_DESCRIPTION.put("segmentExportWorkflow", SEGMENT_EXPORT_STEPS);
        DISPLAY_DESCRIPTION.put("processAnalyzeWorkflow", PROCESS_ANALYZE_STEPS);
        DISPLAY_DESCRIPTION.put("rtsBulkScoreWorkflow", RTS_BULK_SCORE_STEPS);
        DISPLAY_DESCRIPTION.put("cdlDataFeedImportWorkflow", CDL_DATA_FEED_IMPORT_STEPS);
        DISPLAY_DESCRIPTION.put("importMatchAndScoreWorkflow", IMPORT_MATCH_AND_SCORE_STEPS);
        DISPLAY_DESCRIPTION.put("customEventModelingWorkflow", CUSTOM_EVENT_MODELING_STEPS);
        DISPLAY_DESCRIPTION.put("cdlOperationWorkflow", CDL_OPERATION_STEPS);
        DISPLAY_DESCRIPTION.put("importAndRTSBulkScoreWorkflow", IMPORT_AND_RTS_BULK_SCORE_STEPS);
        DISPLAY_DESCRIPTION.put("scoreWorkflow", SCORE_STEPS);
    }

    public static String getMappedName(String workflowType, int stepIndex) {
        try {
            String[] steps = DISPLAY_NAME.get(workflowType);
            return steps[stepIndex];
        } catch (RuntimeException exc) {
            return "no_mapped_step_name";
        }
    }

    public static String getMappedDescription(String workflowType, int stepIndex) {
        try {
            String[] steps = DISPLAY_DESCRIPTION.get(workflowType);
            return steps[stepIndex];
        } catch (RuntimeException exc) {
            return "no_mapped_step_description";
        }
    }

    // update this method when PA steps change
    private static String[] generatePAStepDescriptions() {
        String merge = "Merging, De-duping & matching to Lattice Data Cloud";
        String analyze = "Analyzing";
        String publish = "Publishing";
        String score = "Scoring";

        int totalSteps = 300; // set to a number more than PA steps
        String[] steps = new String[totalSteps];
        int step = 0;

        // Merge Phase: everything before [15] cloneAccount
        while (step < 15) {
            steps[step++] = merge;
        }

        // Analyze Phase: until [77] combineStatistics
        while (step <= 77) {
            steps[step++] = analyze;
        }

        // Publish Phase: 2 publish steps
        steps[step++] = publish;
        steps[step++] = publish;

        // Scoring Phase: every thing after that
        while(step < totalSteps) {
            steps[step++] = score;
        }

        return steps;
    }
}
