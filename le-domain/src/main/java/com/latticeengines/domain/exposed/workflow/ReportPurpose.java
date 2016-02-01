package com.latticeengines.domain.exposed.workflow;

import java.util.HashMap;
import java.util.Map;

public enum ReportPurpose {
    IMPORT_SUMMARY("ImportSummary"), //
    MODEL_SUMMARY("ModelSummary"), //
    INDUSTRY_ATTR_LEVEL_SUMMARY("CreateAttributeLevelSummary_BusinessIndustry"), //
    SUBINDUSTRY_ATTR_LEVEL_SUMMARY("CreateAttributeLevelSummary_BusinessIndustry2"), //
    REVENUE_ATTR_LEVEL_SUMMARY("CreateAttributeLevelSummary_BusinessRevenueRange"), //
    EMPLOYEE_ATTR_LEVEL_SUMMARY("CreateAttributeLevelSummary_BusinessEmployeesRange"), //
    INDUSTRY_LIFT_ATTR_LEVEL_SUMMARY("CreateAttributeLevelSummary_BusinessIndustry_Probability"), //
    SUBINDUSTRY_LIFT_ATTR_LEVEL_SUMMARY("CreateAttributeLevelSummary_BusinessIndustry2_Probability"), //
    REVENUE_LIFT_ATTR_LEVEL_SUMMARY("CreateAttributeLevelSummary_BusinessRevenueRange_Probability"), //
    EMPLOYEE_LIFT_ATTR_LEVEL_SUMMARY("CreateAttributeLevelSummary_BusinessEmployeesRange_Probability");
    
    private String key;
    
    private static Map<String, ReportPurpose> map = new HashMap<>();
    
    static {
        for (ReportPurpose reportPurpose : ReportPurpose.values()) {
            map.put(reportPurpose.getKey(), reportPurpose);
        }
    }
    
    ReportPurpose(String key) {
        this.key = key;
    }
    
    public String getKey() {
        return key;
    }
    
    public static ReportPurpose getReportPurpose(String key) {
        return map.get(key);
    }
    
}
