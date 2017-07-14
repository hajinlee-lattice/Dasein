package com.latticeengines.domain.exposed.admin;

import java.util.HashSet;
import java.util.Set;

import com.latticeengines.domain.exposed.pls.PlsFeatureFlag;

public enum LatticeFeatureFlag {

    DANTE("Dante", "Dante"), //
    QUOTA(true, "Quota", "Quota"), //
    TARGET_MARKET(true, "TargetMarket", "Target Market"), //
    USE_EAI_VALIDATE_CREDENTIAL(true, "ValidateCredsUsingEai", "Use Eai to valiate source credentials"), //
    ENABLE_POC_TRANSFORM(true, "EnablePocTransform", "Enable POC in data transform"), //
    USE_SALESFORCE_SETTINGS(true, "UseSalesforceSettings", "Use Salesforce settings"), //
    USE_MARKETO_SETTINGS(true, "UseMarketoSettings", "Use Marketo settings"), //
    USE_ELOQUA_SETTINGS(true, "UseEloquaSettings", "Use Eloqua settings"), //
    ALLOW_PIVOT_FILE("AllowPivotFile", "Allow pivot file"), //
    ENABLE_CAMPAIGN_UI(true, "EnableCampaignUI", "Enable Campaign UI"), //
    USE_DNB_RTS_AND_MODELING(true, "UseDnbRtsAndModeling", "User DNB RTS and Modeling"), //
    ENABLE_LATTICE_MARKETO_CREDENTIAL_PAGE(true, "EnableLatticeMarketoCredentialPage",
            "Enable Lattice Marketo Credential Page"), //
    ENABLE_DATA_ENCRYPTION("EnableDataEncryption", "Enable data encryption"), //
    ENABLE_INTERNAL_ENRICHMENT_ATTRIBUTES("EnableInternalEnrichmentAttributes",
            "Enable Internal Enrichment Attributes"), //
    ENABLE_DATA_PROFILING_V2(true, "EnableDataProfilingV2", "Enable Data Profiling Version 2"), //
    ENABLE_FUZZY_MATCH(true, "EnableFuzzyMatch", "Enable Fuzzy Match"), //
    LATTICE_INSIGHTS("LatticeInsights", "Lattice Insights"), //
    BYPASS_DNB_CACHE(true, "BypassDnbCache", "Bypass DnB Cache"), //
    ENABLE_CDL("EnableCdl", "Enable Customer Data Lake"), //
    ENABLE_MATCH_DEBUG("EnableMatchDebug", "Enable Match Debug"), //
    ENABLE_TALKING_POINTS("EnableTalkingPoints", "Enable Talking Points"); //

    private String name;
    private String documentation;
    private static Set<String> names;
    private boolean deprecated = false;

    LatticeFeatureFlag(String name, String documentation) {
        this.name = name;
        this.documentation = documentation;
    }

    LatticeFeatureFlag(boolean deprecated, String name, String documentation) {
        this.name = name;
        this.documentation = documentation;
        this.deprecated = deprecated;
    }

    public String getName() {
        return this.name;
    }

    public String getDocumentation() {
        return this.documentation;
    }

    public boolean isDeprecated() {
        return deprecated;
    }

    static {
        names = new HashSet<>();
        for (PlsFeatureFlag flag : PlsFeatureFlag.values()) {
            names.add(flag.getName());
        }
    }

}
