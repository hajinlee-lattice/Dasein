package com.latticeengines.pls.service;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class VdbMetadataConstants {

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
    public static final String ATTRIBUTE_DISPLAY_DISCRETIZATION = "DisplayDiscretizationStrategy";
    public static final String ATTRIBUTE_STATISTICAL_TYPE = "StatisticalType";

    public static final String ATTRIBUTE_NULL_VALUE = "<NULL>";

    public static final String CATEGORY_OPTIONS[] = { "Lead Information", "Marketing Activity" };
    public static final String APPROVED_USAGE_OPTIONS[] = { "None", "Model", "ModelAndAllInsights", "ModelAndModelInsights" };
    public static final String FUNDAMENTAL_TYPE_OPTIONS[] = { "alpha", "boolean", "currency", "numeric", "percentage", "year" };
    public static final String STATISTICAL_TYPE_OPTIONS[] = { "interval", "nominal", "ordinal", "ratio" };

    @SuppressWarnings("serial")
    public static final Map<String, String> SOURCE_MAPPING = Collections.unmodifiableMap(
            new HashMap<String, String>() {{
                put("MKTO_ActivityRecord_Import", SOURCE_MARKETO);
                put("MKTO_LeadRecord_Import", SOURCE_MARKETO);
                put("Marketo", SOURCE_MARKETO);

                put("ELQ_Contact_Import", SOURCE_ELOQUA);
                put("ELQ_Contact_Diagnostic_Import", SOURCE_ELOQUA);
                put("ELQ_Contact_Validation_Import", SOURCE_ELOQUA);
                put("Eloqua", SOURCE_ELOQUA);

                put("SFDC_Account_Import", SOURCE_SALESFORCE);
                put("SFDC_Campaign_Import", SOURCE_SALESFORCE);
                put("SFDC_CampaignMember_Import", SOURCE_SALESFORCE);
                put("SFDC_Contact_Import", SOURCE_SALESFORCE);
                put("SFDC_Lead_Import", SOURCE_SALESFORCE);
                put("SFDC_Opportunity_Import", SOURCE_SALESFORCE);
                put("SFDC_OpportunityContactRole_Import", SOURCE_SALESFORCE);
                put("SFDC_OpportunityHistory_Import", SOURCE_SALESFORCE);
                put("SFDC_OpportunityLineItem_Import", SOURCE_SALESFORCE);
                put("SFDC_Product2_Import", SOURCE_SALESFORCE);
                put("SFDC_User_Import", SOURCE_SALESFORCE);
                put("SFDC", SOURCE_SALESFORCE);

                put("PD_Alexa_Source_Import", SOURCE_LATTICE_DATA_CLOUD);
                put("Alexa_Source", SOURCE_LATTICE_DATA_CLOUD);
                put("PD_DerivedColumns_Import", SOURCE_LATTICE_DATA_CLOUD);
                put("DerivedColumns", SOURCE_LATTICE_DATA_CLOUD);
                put("PD_Builtwith_Source_Import", SOURCE_LATTICE_DATA_CLOUD);
                put("Builtwith_Source", SOURCE_LATTICE_DATA_CLOUD);
                put("PD_HGData_Source_Import", SOURCE_LATTICE_DATA_CLOUD);
                put("HGData_Source", SOURCE_LATTICE_DATA_CLOUD);
                put("PD_OrbIntelligence_Source_Import", SOURCE_LATTICE_DATA_CLOUD);
                put("OrbIntelligence_Source", SOURCE_LATTICE_DATA_CLOUD);
                put("PD_Experian_Source_Import", SOURCE_LATTICE_DATA_CLOUD);
                put("Experian_Source", SOURCE_LATTICE_DATA_CLOUD);
                put("PD_LexisNexis_Source_Import", SOURCE_LATTICE_DATA_CLOUD);
                put("LexisNexis_Source", SOURCE_LATTICE_DATA_CLOUD);
                put("PD_HPA_New_Source_Import", SOURCE_LATTICE_DATA_CLOUD);
                put("HPA_New_Source", SOURCE_LATTICE_DATA_CLOUD);
                put("PD_HPA_Plus_Source_Import", SOURCE_LATTICE_DATA_CLOUD);
                put("HPA_Plus_Source", SOURCE_LATTICE_DATA_CLOUD);
                put("Info_PublicDomain", SOURCE_LATTICE_DATA_CLOUD);
                put("Info_PublicDomain_Import", SOURCE_LATTICE_DATA_CLOUD);
                put("Sys_LatticeSystemID", SOURCE_LATTICE_DATA_CLOUD);
                put("Sys_LatticeSystemID_Import", SOURCE_LATTICE_DATA_CLOUD);
                put("RptDB_Scorehistory", SOURCE_LATTICE_DATA_CLOUD);
                put("RptDB_Scorehistory_Import", SOURCE_LATTICE_DATA_CLOUD);

            }
        }
    );
}