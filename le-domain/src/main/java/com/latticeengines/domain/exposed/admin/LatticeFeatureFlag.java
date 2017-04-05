package com.latticeengines.domain.exposed.admin;

import java.util.HashSet;
import java.util.Set;

import com.latticeengines.domain.exposed.pls.PlsFeatureFlag;

public enum LatticeFeatureFlag {

    DANTE("Dante", "Dante"), //
    QUOTA("Quota", "Quota"), //
    TARGET_MARKET("TargetMarket", "Target Market"), //
    USE_EAI_VALIDATE_CREDENTIAL("ValidateCredsUsingEai", "Use Eai to valiate source credentials"), //
    ENABLE_POC_TRANSFORM("EnablePocTransform", "Enable POC in data transform"), //
    USE_SALESFORCE_SETTINGS("UseSalesforceSettings", "Use Salesforce settings"), //
    USE_MARKETO_SETTINGS("UseMarketoSettings", "Use Marketo settings"), //
    USE_ELOQUA_SETTINGS("UseEloquaSettings", "Use Eloqua settings"), //
    ALLOW_PIVOT_FILE("AllowPivotFile", "Allow pivot file"), //
    ENABLE_CAMPAIGN_UI("EnableCampaignUI", "Enable Campaign UI"), //
    USE_DNB_RTS_AND_MODELING("UseDnbRtsAndModeling", "User DNB RTS and Modeling"), //
    ENABLE_LATTICE_MARKETO_CREDENTIAL_PAGE("EnableLatticeMarketoCredentialPage",
            "Enable Lattice Marketo Credential Page"), //
    ENABLE_DATA_ENCRYPTION("EnableDataEncryption", "Enable data encryption"), //
    ENABLE_INTERNAL_ENRICHMENT_ATTRIBUTES("EnableInternalEnrichmentAttributes",
            "Enable Internal Enrichment Attributes"), //
    ENABLE_DATA_PROFILING_V2("EnableDataProfilingV2", "Enable Data Profiling Version 2"), //
    ENABLE_FUZZY_MATCH("EnableFuzzyMatch", "Enable Fuzzy Match"), //
    LATTICE_INSIGHTS("LatticeInsights", "Lattice Insights"), //
    BYPASS_DNB_CACHE("BypassDnbCache", "Bypass DnB Cache"), //
    ENABLE_CDL("EnableCdl", "Enable Customer Data Lake"), //
    ENABLE_MATCH_DEBUG("EnableMatchDebug", "Enable Match Debug");

    private String name;
    private String documentation;
    private static Set<String> names;

    LatticeFeatureFlag(String name, String documentation) {
        this.name = name;
        this.documentation = documentation;
    }

    public String getName() {
        return this.name;
    }

    public String getDocumentation() {
        return this.documentation;
    }

    static {
        names = new HashSet<>();
        for (PlsFeatureFlag flag : PlsFeatureFlag.values()) {
            names.add(flag.getName());
        }
    }

}
