package com.latticeengines.pls.service;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public interface MetadataConstants {

    static final String SOURCE_MARKETO = "Marketo";
    static final String SOURCE_ELOQUA = "Eloqua";
    static final String SOURCE_SALESFORCE = "Salesforce";
    static final String SOURCE_LATTICE_DATA_CLOUD = "Lattice Data Cloud";

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

            }
        }
    );
}