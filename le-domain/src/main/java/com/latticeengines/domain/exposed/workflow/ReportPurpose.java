package com.latticeengines.domain.exposed.workflow;

import java.util.HashMap;
import java.util.Map;

public enum ReportPurpose {
    /**
     * Prospect Discovery
     */
    IMPORT_SUMMARY("ImportSummary"), //
    MODEL_SUMMARY("ModelSummary"), //
    INDUSTRY_ATTR_LEVEL_SUMMARY("CreateAttributeLevelSummary_BusinessIndustry"), //
    SUBINDUSTRY_ATTR_LEVEL_SUMMARY("CreateAttributeLevelSummary_BusinessIndustry2"), //
    REVENUE_ATTR_LEVEL_SUMMARY("CreateAttributeLevelSummary_BusinessRevenueRange"), //
    EMPLOYEE_ATTR_LEVEL_SUMMARY("CreateAttributeLevelSummary_BusinessEmployeesRange"), //
    INDUSTRY_LIFT_ATTR_LEVEL_SUMMARY("CreateAttributeLevelSummary_BusinessIndustry_Probability"), //
    SUBINDUSTRY_LIFT_ATTR_LEVEL_SUMMARY(
            "CreateAttributeLevelSummary_BusinessIndustry2_Probability"), //
    REVENUE_LIFT_ATTR_LEVEL_SUMMARY("CreateAttributeLevelSummary_BusinessRevenueRange_Probability"), //
    EMPLOYEE_LIFT_ATTR_LEVEL_SUMMARY(
            "CreateAttributeLevelSummary_BusinessEmployeesRange_Probability"), //

    /**
     * Lead Prioritization
     */
    PREMATCH_EVENT_TABLE_SUMMARY("PreMatchEventTableSummary"), //

    /**
     * CDL
     *
     */
    CONSOLIDATE_RECORDS_SUMMARY("ConsolidateRecordsSummary"), //
    PROCESS_ANALYZE_RECORDS_SUMMARY("ProcessAnalyzeRecordsSummary"), //
    PROCESS_ANALYZE_DECISIONS_SUMMARY("ProcessAnalyzeDecisionsSummary"), //
    MAINTENANCE_OPERATION_SUMMARY("MaintenanceOperationSummary"), //
    ENTITY_STATS_SUMMARY("EntityStatsSummary"), //
    ENTITY_MATCH_SUMMARY("EntityMatchSummary"), //
    /**
     * Generic
     */
    IMPORT_DATA_SUMMARY("ImportDataSummary"), //
    EVENT_TABLE_IMPORT_SUMMARY("EventTableImportSummary"), //
    PUBLISH_DATA_SUMMARY("PublishDataSummary"), //
    ENTITIES_SUMMARY("EntitiesSummary"), //
    SYSTEM_ACTIONS("SystemActions"); //

    private static Map<String, ReportPurpose> map = new HashMap<>();

    static {
        for (ReportPurpose reportPurpose : ReportPurpose.values()) {
            map.put(reportPurpose.getKey(), reportPurpose);
        }
    }

    private String key;

    ReportPurpose(String key) {
        this.key = key;
    }

    public static ReportPurpose getReportPurpose(String key) {
        return map.get(key);
    }

    public String getKey() {
        return key;
    }

}
