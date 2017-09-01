package com.latticeengines.domain.exposed.pls.frontend;

import java.util.HashMap;
import java.util.Map;

public final class JobStepDisplayInfoMapping {
    private static final Map<String, String> DISPLAY_NAME_MAP = new HashMap<>();
    static {
        DISPLAY_NAME_MAP.put("fitmodelworkflow.markreportoutofdate", "load_data");
        DISPLAY_NAME_MAP.put("fitmodelworkflow.importdata", "load_data");
        DISPLAY_NAME_MAP.put("fitmodelworkflow.createprematcheventtable", "match_data");
        DISPLAY_NAME_MAP.put("fitmodelworkflow.loadhdfstabletopdserver", "match_data");
        DISPLAY_NAME_MAP.put("fitmodelworkflow.match", "match_data");
        DISPLAY_NAME_MAP.put("fitmodelworkflow.createeventtablefrommatchresult", "generate_insights");
        DISPLAY_NAME_MAP.put("fitmodelworkflow.runimportsummarydataflow", "generate_insights");
        DISPLAY_NAME_MAP.put("fitmodelworkflow.registerimportsummaryreport", "generate_insights");
        DISPLAY_NAME_MAP.put("fitmodelworkflow.sample", "generate_insights");
        DISPLAY_NAME_MAP.put("fitmodelworkflow.profileandmodel", "create_global_model");
        DISPLAY_NAME_MAP.put("fitmodelworkflow.choosemodel", "create_global_model");
        DISPLAY_NAME_MAP.put("fitmodelworkflow.activatemodel", "create_global_model");
        DISPLAY_NAME_MAP.put("fitmodelworkflow.runscoretabledataflow", "create_global_target_market");
        DISPLAY_NAME_MAP.put("fitmodelworkflow.runattributelevelsummarydataflows", "create_global_target_market");
        DISPLAY_NAME_MAP.put("fitmodelworkflow.scoreeventtable", "score_training_set");
        DISPLAY_NAME_MAP.put("fitmodelworkflow.combineinputtablewithscore", "score_training_set");
        DISPLAY_NAME_MAP.put("fitmodelworkflow.pivotscoreandevent", "score_training_set");

        DISPLAY_NAME_MAP.put("importmatchandmodelworkflow.importdata", "load_data");
        DISPLAY_NAME_MAP.put("importmatchandmodelworkflow.createeventtablereport", "load_data");
        DISPLAY_NAME_MAP.put("importmatchandmodelworkflow.createprematcheventtablereport", "generate_insights");
        DISPLAY_NAME_MAP.put("importmatchandmodelworkflow.validateprematcheventtable", "generate_insights");
        DISPLAY_NAME_MAP.put("importmatchandmodelworkflow.prematchstep", "generate_insights");
        DISPLAY_NAME_MAP.put("importmatchandmodelworkflow.preparebulkmatchinput", "generate_insights");
        DISPLAY_NAME_MAP.put("importmatchandmodelworkflow.parallelblockexecution", "generate_insights");
        DISPLAY_NAME_MAP.put("importmatchandmodelworkflow.processmatchresult", "generate_insights");
        DISPLAY_NAME_MAP.put("importmatchandmodelworkflow.dedupeventtabledataflow", "generate_insights");
        DISPLAY_NAME_MAP.put("importmatchandmodelworkflow.addstandardattributesdataflow",
                "create_global_target_market");
        DISPLAY_NAME_MAP.put("importmatchandmodelworkflow.sample", "create_global_target_market");
        DISPLAY_NAME_MAP.put("importmatchandmodelworkflow.exportdata", "create_global_target_market");
        DISPLAY_NAME_MAP.put("importmatchandmodelworkflow.setmatchselection", "create_global_target_market");
        DISPLAY_NAME_MAP.put("importmatchandmodelworkflow.writemetadatafiles", "create_global_target_market");
        DISPLAY_NAME_MAP.put("importmatchandmodelworkflow.profile", "create_global_target_market");
        DISPLAY_NAME_MAP.put("importmatchandmodelworkflow.reviewmodel", "create_global_target_market");
        DISPLAY_NAME_MAP.put("importmatchandmodelworkflow.remediatedatarules", "create_global_target_market");
        DISPLAY_NAME_MAP.put("importmatchandmodelworkflow.writemetadatafiles", "create_global_target_market");
        DISPLAY_NAME_MAP.put("importmatchandmodelworkflow.createmodel", "create_global_target_market");
        DISPLAY_NAME_MAP.put("importmatchandmodelworkflow.downloadandprocessmodelsummaries",
                "create_global_target_market");
        DISPLAY_NAME_MAP.put("importmatchandmodelworkflow.createnote", "create_global_target_market");
        DISPLAY_NAME_MAP.put("importmatchandmodelworkflow.persistdatarules", "create_global_target_market");
        DISPLAY_NAME_MAP.put("importmatchandmodelworkflow.setconfigurationforscoring", "create_global_target_market");
        DISPLAY_NAME_MAP.put("importmatchandmodelworkflow.prematchstep", "create_global_target_market");
        DISPLAY_NAME_MAP.put("importmatchandmodelworkflow.preparebulkmatchinput", "create_global_target_market");
        DISPLAY_NAME_MAP.put("importmatchandmodelworkflow.parallelblockexecution", "score_training_set");
        DISPLAY_NAME_MAP.put("importmatchandmodelworkflow.processmatchresult", "score_training_set");
        DISPLAY_NAME_MAP.put("importmatchandmodelworkflow.rtsscoreeventtable", "score_training_set");
        DISPLAY_NAME_MAP.put("importmatchandmodelworkflow.combinematchdebugwithscoredataflow", "score_training_set");
        DISPLAY_NAME_MAP.put("importmatchandmodelworkflow.combineinputtablewithscoredataflow", "score_training_set");
        DISPLAY_NAME_MAP.put("importmatchandmodelworkflow.exportdata", "score_training_set");
        DISPLAY_NAME_MAP.put("importmatchandmodelworkflow.pivotscoreandeventdataflow", "score_training_set");
        DISPLAY_NAME_MAP.put("importmatchandmodelworkflow.exportdata", "score_training_set");

        DISPLAY_NAME_MAP.put("modelandemailworkflow.dedupeventtable", "load_data");
        DISPLAY_NAME_MAP.put("modelandemailworkflow.matchdatacloud", "load_data");
        DISPLAY_NAME_MAP.put("modelandemailworkflow.processmatchresult", "generate_insights");
        DISPLAY_NAME_MAP.put("modelandemailworkflow.addstandardattributes", "generate_insights");
        DISPLAY_NAME_MAP.put("modelandemailworkflow.resolvemetadatafromuserrefinedattributes",
                "create_global_target_market");
        DISPLAY_NAME_MAP.put("modelandemailworkflow.sample", "create_global_target_market");
        DISPLAY_NAME_MAP.put("modelandemailworkflow.exportdata", "create_global_target_market");
        DISPLAY_NAME_MAP.put("modelandemailworkflow.setmatchselection", "create_global_target_market");
        DISPLAY_NAME_MAP.put("modelandemailworkflow.writemetadatafiles", "create_global_target_market");
        DISPLAY_NAME_MAP.put("modelandemailworkflow.profile", "create_global_target_market");
        DISPLAY_NAME_MAP.put("modelandemailworkflow.reviewmodel", "create_global_target_market");
        DISPLAY_NAME_MAP.put("modelandemailworkflow.remediatedatarules", "create_global_target_market");
        DISPLAY_NAME_MAP.put("modelandemailworkflow.createmodel", "create_global_target_market");
        DISPLAY_NAME_MAP.put("modelandemailworkflow.downloadandprocessmodelsummaries", "create_global_target_market");
        DISPLAY_NAME_MAP.put("modelandemailworkflow.persistdatarules", "create_global_target_market");
        DISPLAY_NAME_MAP.put("modelandemailworkflow.setconfigurationforscoring", "score_training_set");
        DISPLAY_NAME_MAP.put("modelandemailworkflow.score", "score_training_set");
        DISPLAY_NAME_MAP.put("modelandemailworkflow.scoreeventtable", "score_training_set");
        DISPLAY_NAME_MAP.put("modelandemailworkflow.combineinputtablewithscore", "score_training_set");
        DISPLAY_NAME_MAP.put("modelandemailworkflow.pivotscoreandevent", "score_training_set");

        DISPLAY_NAME_MAP.put("pmmlmodelworkflow.createpmmlmodel", "create_global_target_market");
    }

    private static final Map<String, String> DISPLAY_DESC_MAP = new HashMap<>();
    static {
        DISPLAY_DESC_MAP.put("fitmodelworkflow.markreportoutofdate", "load_data");
        DISPLAY_DESC_MAP.put("fitmodelworkflow.importdata", "load_data");
        DISPLAY_DESC_MAP.put("fitmodelworkflow.createprematcheventtable", "match_data");
        DISPLAY_DESC_MAP.put("fitmodelworkflow.loadhdfstabletopdserver", "match_data");
        DISPLAY_DESC_MAP.put("fitmodelworkflow.match", "match_data");
        DISPLAY_DESC_MAP.put("fitmodelworkflow.createeventtablefrommatchresult", "generate_insights");
        DISPLAY_DESC_MAP.put("fitmodelworkflow.runimportsummarydataflow", "generate_insights");
        DISPLAY_DESC_MAP.put("fitmodelworkflow.registerimportsummaryreport", "generate_insights");
        DISPLAY_DESC_MAP.put("fitmodelworkflow.sample", "generate_insights");
        DISPLAY_DESC_MAP.put("fitmodelworkflow.profileandmodel", "create_global_model");
        DISPLAY_DESC_MAP.put("fitmodelworkflow.choosemodel", "create_global_model");
        DISPLAY_DESC_MAP.put("fitmodelworkflow.activatemodel", "create_global_model");
        DISPLAY_DESC_MAP.put("fitmodelworkflow.runscoretabledataflow", "create_global_target_market");
        DISPLAY_DESC_MAP.put("fitmodelworkflow.runattributelevelsummarydataflows", "create_global_target_market");
        DISPLAY_DESC_MAP.put("fitmodelworkflow.scoreeventtable", "score_training_set");
        DISPLAY_DESC_MAP.put("fitmodelworkflow.combineinputtablewithscore", "score_training_set");
        DISPLAY_DESC_MAP.put("fitmodelworkflow.pivotscoreandevent", "score_training_set");

        DISPLAY_DESC_MAP.put("importmatchandmodelworkflow.importdata", "load_data");
        DISPLAY_DESC_MAP.put("importmatchandmodelworkflow.createeventtablereport", "load_data");
        DISPLAY_DESC_MAP.put("importmatchandmodelworkflow.createprematcheventtablereport", "generate_insights");
        DISPLAY_DESC_MAP.put("importmatchandmodelworkflow.validateprematcheventtable", "generate_insights");
        DISPLAY_DESC_MAP.put("importmatchandmodelworkflow.prematchstep", "generate_insights");
        DISPLAY_DESC_MAP.put("importmatchandmodelworkflow.preparebulkmatchinput", "generate_insights");
        DISPLAY_DESC_MAP.put("importmatchandmodelworkflow.parallelblockexecution", "generate_insights");
        DISPLAY_DESC_MAP.put("importmatchandmodelworkflow.processmatchresult", "generate_insights");
        DISPLAY_DESC_MAP.put("importmatchandmodelworkflow.dedupeventtabledataflow", "generate_insights");
        DISPLAY_DESC_MAP.put("importmatchandmodelworkflow.addstandardattributesdataflow",
                "create_global_target_market");
        DISPLAY_DESC_MAP.put("importmatchandmodelworkflow.sample", "create_global_target_market");
        DISPLAY_DESC_MAP.put("importmatchandmodelworkflow.exportdata", "create_global_target_market");
        DISPLAY_DESC_MAP.put("importmatchandmodelworkflow.setmatchselection", "create_global_target_market");
        DISPLAY_DESC_MAP.put("importmatchandmodelworkflow.writemetadatafiles", "create_global_target_market");
        DISPLAY_DESC_MAP.put("importmatchandmodelworkflow.profile", "create_global_target_market");
        DISPLAY_DESC_MAP.put("importmatchandmodelworkflow.reviewmodel", "create_global_target_market");
        DISPLAY_DESC_MAP.put("importmatchandmodelworkflow.remediatedatarules", "create_global_target_market");
        DISPLAY_DESC_MAP.put("importmatchandmodelworkflow.writemetadatafiles", "create_global_target_market");
        DISPLAY_DESC_MAP.put("importmatchandmodelworkflow.createmodel", "create_global_target_market");
        DISPLAY_DESC_MAP.put("importmatchandmodelworkflow.downloadandprocessmodelsummaries",
                "create_global_target_market");
        DISPLAY_DESC_MAP.put("importmatchandmodelworkflow.createnote", "create_global_target_market");
        DISPLAY_DESC_MAP.put("importmatchandmodelworkflow.persistdatarules", "create_global_target_market");
        DISPLAY_DESC_MAP.put("importmatchandmodelworkflow.setconfigurationforscoring", "create_global_target_market");
        DISPLAY_DESC_MAP.put("importmatchandmodelworkflow.prematchstep", "create_global_target_market");
        DISPLAY_DESC_MAP.put("importmatchandmodelworkflow.preparebulkmatchinput", "create_global_target_market");
        DISPLAY_DESC_MAP.put("importmatchandmodelworkflow.parallelblockexecution", "score_training_set");
        DISPLAY_DESC_MAP.put("importmatchandmodelworkflow.processmatchresult", "score_training_set");
        DISPLAY_DESC_MAP.put("importmatchandmodelworkflow.rtsscoreeventtable", "score_training_set");
        DISPLAY_DESC_MAP.put("importmatchandmodelworkflow.combinematchdebugwithscoredataflow", "score_training_set");
        DISPLAY_DESC_MAP.put("importmatchandmodelworkflow.combineinputtablewithscoredataflow", "score_training_set");
        DISPLAY_DESC_MAP.put("importmatchandmodelworkflow.exportdata", "score_training_set");
        DISPLAY_DESC_MAP.put("importmatchandmodelworkflow.pivotscoreandeventdataflow", "score_training_set");
        DISPLAY_DESC_MAP.put("importmatchandmodelworkflow.exportdata", "score_training_set");

        DISPLAY_DESC_MAP.put("modelandemailworkflow.dedupeventtable", "load_data");
        DISPLAY_DESC_MAP.put("modelandemailworkflow.matchdatacloud", "load_data");
        DISPLAY_DESC_MAP.put("modelandemailworkflow.processmatchresult", "generate_insights");
        DISPLAY_DESC_MAP.put("modelandemailworkflow.addstandardattributes", "generate_insights");
        DISPLAY_DESC_MAP.put("modelandemailworkflow.resolvemetadatafromuserrefinedattributes",
                "create_global_target_market");
        DISPLAY_DESC_MAP.put("modelandemailworkflow.sample", "create_global_target_market");
        DISPLAY_DESC_MAP.put("modelandemailworkflow.exportdata", "create_global_target_market");
        DISPLAY_DESC_MAP.put("modelandemailworkflow.setmatchselection", "create_global_target_market");
        DISPLAY_DESC_MAP.put("modelandemailworkflow.writemetadatafiles", "create_global_target_market");
        DISPLAY_DESC_MAP.put("modelandemailworkflow.profile", "create_global_target_market");
        DISPLAY_DESC_MAP.put("modelandemailworkflow.reviewmodel", "create_global_target_market");
        DISPLAY_DESC_MAP.put("modelandemailworkflow.remediatedatarules", "create_global_target_market");
        DISPLAY_DESC_MAP.put("modelandemailworkflow.createmodel", "create_global_target_market");
        DISPLAY_DESC_MAP.put("modelandemailworkflow.downloadandprocessmodelsummaries", "create_global_target_market");
        DISPLAY_DESC_MAP.put("modelandemailworkflow.persistdatarules", "create_global_target_market");
        DISPLAY_DESC_MAP.put("modelandemailworkflow.setconfigurationforscoring", "score_training_set");
        DISPLAY_DESC_MAP.put("modelandemailworkflow.score", "score_training_set");
        DISPLAY_DESC_MAP.put("modelandemailworkflow.scoreeventtable", "score_training_set");
        DISPLAY_DESC_MAP.put("modelandemailworkflow.combineinputtablewithscore", "score_training_set");
        DISPLAY_DESC_MAP.put("modelandemailworkflow.pivotscoreandevent", "score_training_set");

        DISPLAY_DESC_MAP.put("pmmlmodelworkflow.createpmmlmodel", "create_global_target_market");
    }

    public static String getMappedName(String workflowType, String workflowStepType) {
        String key = constructMapKey(workflowType, workflowStepType);
        return DISPLAY_NAME_MAP.getOrDefault(key, "no_mapped_step_name");
    }

    public static String getMappedDescription(String workflowType, String workflowStepType) {
        String key = constructMapKey(workflowType, workflowStepType);
        return DISPLAY_DESC_MAP.getOrDefault(key, "no_mapped_step_description");
    }

    private static String constructMapKey(String workflowType, String workflowStepType) {
        return workflowType.toLowerCase() + "." + workflowStepType.toLowerCase();
    }
}
