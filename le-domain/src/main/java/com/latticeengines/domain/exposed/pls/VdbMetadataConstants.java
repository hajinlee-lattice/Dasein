package com.latticeengines.domain.exposed.pls;

import java.util.HashMap;
import java.util.Map;

public final class VdbMetadataConstants {

    protected VdbMetadataConstants() {
        throw new UnsupportedOperationException();
    }

    public static final String MODELING_QUERY_NAME = "Q_PLS_Modeling";
    public static final String CUSTOM_QUERY_NAME = "Q_Metadata_Custom";

    public static final String SOURCE_MARKETO = "Marketo";
    public static final String SOURCE_ELOQUA = "Eloqua";
    public static final String SOURCE_SALESFORCE = "Salesforce";
    public static final String SOURCE_LATTICE_DATA_CLOUD = "Lattice Data Cloud";
    public static final String SOURCE_LATTICE_DATA_SCIENCE = "Lattice Data Science";

    public static final String ATTRIBUTE_SOURCE = "DataSource";
    public static final String ATTRIBUTE_CATEGORY = "Category";
    public static final String ATTRIBUTE_DISPLAYNAME = "DisplayName";
    public static final String ATTRIBUTE_DESCRIPTION = "Description";
    public static final String ATTRIBUTE_APPROVED_USAGE = "ApprovedUsage";
    public static final String ATTRIBUTE_TAGS = "Tags";
    public static final String ATTRIBUTE_FUNDAMENTAL_TYPE = "FundamentalType";
    public static final String ATTRIBUTE_FUNDAMENTAL_UNKNOWN_VALUE = "Unknown";
    public static final String ATTRIBUTE_DISPLAY_DISCRETIZATION = "DisplayDiscretizationStrategy";
    public static final String ATTRIBUTE_STATISTICAL_TYPE = "StatisticalType";

    public static final String ATTRIBUTE_NULL_VALUE = "<NULL>";

    public static final String[] CATEGORY_OPTIONS = { "Lead Information", "Marketing Activity" };
    public static final String[] APPROVED_USAGE_OPTIONS = { "None", "Model", "ModelAndAllInsights",
            "ModelAndModelInsights" };
    public static final String[] FUNDAMENTAL_TYPE_OPTIONS = { "alpha", "boolean", "currency",
            "numeric", "percentage", "year" };
    public static final String[] STATISTICAL_TYPE_OPTIONS = { "interval", "nominal", "ordinal",
            "ratio" };

    public static final Map<String, String> SOURCE_MAPPING = new HashMap<>();
    static {
        SOURCE_MAPPING.put("MKTO_ActivityRecord_Import", SOURCE_MARKETO);
        SOURCE_MAPPING.put("MKTO_LeadRecord_Import", SOURCE_MARKETO);
        SOURCE_MAPPING.put("Marketo", SOURCE_MARKETO);

        SOURCE_MAPPING.put("ELQ_Contact_Import", SOURCE_ELOQUA);
        SOURCE_MAPPING.put("ELQ_Contact_Diagnostic_Import", SOURCE_ELOQUA);
        SOURCE_MAPPING.put("ELQ_Contact_Validation_Import", SOURCE_ELOQUA);
        SOURCE_MAPPING.put("Eloqua", SOURCE_ELOQUA);

        SOURCE_MAPPING.put("SFDC_Account_Import", SOURCE_SALESFORCE);
        SOURCE_MAPPING.put("SFDC_Campaign_Import", SOURCE_SALESFORCE);
        SOURCE_MAPPING.put("SFDC_CampaignMember_Import", SOURCE_SALESFORCE);
        SOURCE_MAPPING.put("SFDC_Contact_Import", SOURCE_SALESFORCE);
        SOURCE_MAPPING.put("SFDC_Lead_Import", SOURCE_SALESFORCE);
        SOURCE_MAPPING.put("SFDC_Opportunity_Import", SOURCE_SALESFORCE);
        SOURCE_MAPPING.put("SFDC_OpportunityContactRole_Import", SOURCE_SALESFORCE);
        SOURCE_MAPPING.put("SFDC_OpportunityHistory_Import", SOURCE_SALESFORCE);
        SOURCE_MAPPING.put("SFDC_OpportunityLineItem_Import", SOURCE_SALESFORCE);
        SOURCE_MAPPING.put("SFDC_Product2_Import", SOURCE_SALESFORCE);
        SOURCE_MAPPING.put("SFDC_User_Import", SOURCE_SALESFORCE);
        SOURCE_MAPPING.put("SFDC", SOURCE_SALESFORCE);

        SOURCE_MAPPING.put("PD_Alexa_Source_Import", SOURCE_LATTICE_DATA_CLOUD);
        SOURCE_MAPPING.put("Alexa_Source", SOURCE_LATTICE_DATA_CLOUD);
        SOURCE_MAPPING.put("PD_DerivedColumns_Import", SOURCE_LATTICE_DATA_CLOUD);
        SOURCE_MAPPING.put("DerivedColumns", SOURCE_LATTICE_DATA_CLOUD);
        SOURCE_MAPPING.put("PD_Builtwith_Source_Import", SOURCE_LATTICE_DATA_CLOUD);
        SOURCE_MAPPING.put("Builtwith_Source", SOURCE_LATTICE_DATA_CLOUD);
        SOURCE_MAPPING.put("PD_HGData_Source_Import", SOURCE_LATTICE_DATA_CLOUD);
        SOURCE_MAPPING.put("HGData_Source", SOURCE_LATTICE_DATA_CLOUD);
        SOURCE_MAPPING.put("PD_OrbIntelligence_Source_Import", SOURCE_LATTICE_DATA_CLOUD);
        SOURCE_MAPPING.put("OrbIntelligence_Source", SOURCE_LATTICE_DATA_CLOUD);
        SOURCE_MAPPING.put("PD_Experian_Source_Import", SOURCE_LATTICE_DATA_CLOUD);
        SOURCE_MAPPING.put("Experian_Source", SOURCE_LATTICE_DATA_CLOUD);
        SOURCE_MAPPING.put("PD_LexisNexis_Source_Import", SOURCE_LATTICE_DATA_CLOUD);
        SOURCE_MAPPING.put("LexisNexis_Source", SOURCE_LATTICE_DATA_CLOUD);
        SOURCE_MAPPING.put("PD_HPA_New_Source_Import", SOURCE_LATTICE_DATA_CLOUD);
        SOURCE_MAPPING.put("HPA_New_Source", SOURCE_LATTICE_DATA_CLOUD);
        SOURCE_MAPPING.put("PD_HPA_Plus_Source_Import", SOURCE_LATTICE_DATA_CLOUD);
        SOURCE_MAPPING.put("HPA_Plus_Source", SOURCE_LATTICE_DATA_CLOUD);
        SOURCE_MAPPING.put("Info_PublicDomain", SOURCE_LATTICE_DATA_CLOUD);
        SOURCE_MAPPING.put("Info_PublicDomain_Import", SOURCE_LATTICE_DATA_CLOUD);
        SOURCE_MAPPING.put("Sys_LatticeSystemID", SOURCE_LATTICE_DATA_CLOUD);
        SOURCE_MAPPING.put("Sys_LatticeSystemID_Import", SOURCE_LATTICE_DATA_CLOUD);
        SOURCE_MAPPING.put("RptDB_Scorehistory", SOURCE_LATTICE_DATA_CLOUD);
        SOURCE_MAPPING.put("RptDB_Scorehistory_Import", SOURCE_LATTICE_DATA_CLOUD);
    }
}
