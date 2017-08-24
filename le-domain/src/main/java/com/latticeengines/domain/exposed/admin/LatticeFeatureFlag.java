package com.latticeengines.domain.exposed.admin;

import java.util.HashSet;
import java.util.Set;

import com.latticeengines.domain.exposed.pls.PlsFeatureFlag;

public enum LatticeFeatureFlag {


    DANTE("Dante", "Dante"), //
    ALLOW_PIVOT_FILE("AllowPivotFile", "Allow pivot file"), //
    ENABLE_DATA_ENCRYPTION("EnableDataEncryption", "Enable data encryption"), //
    ENABLE_INTERNAL_ENRICHMENT_ATTRIBUTES("EnableInternalEnrichmentAttributes",
            "Enable Internal Enrichment Attributes"), //
    LATTICE_INSIGHTS("LatticeInsights", "Lattice Insights"), //
    ENABLE_MATCH_DEBUG("EnableMatchDebug", "Enable Match Debug"), //
    ENABLE_LPI_PLAYMAKER("EnableLpiPlaymaker", "Enable LPI Playmaker"), //

    @Deprecated
    QUOTA(true, "Quota", "Quota"), //
    @Deprecated
    TARGET_MARKET(true, "TargetMarket", "Target Market"), //
    @Deprecated
    USE_EAI_VALIDATE_CREDENTIAL(true, "ValidateCredsUsingEai", "Use Eai to valiate source credentials"), //
    @Deprecated
    ENABLE_POC_TRANSFORM(true, "EnablePocTransform", "Enable POC in data transform"), //
    @Deprecated
    USE_SALESFORCE_SETTINGS(true, "UseSalesforceSettings", "Use Salesforce settings"), //
    @Deprecated
    USE_MARKETO_SETTINGS(true, "UseMarketoSettings", "Use Marketo settings"), //
    @Deprecated
    USE_ELOQUA_SETTINGS(true, "UseEloquaSettings", "Use Eloqua settings"), //
    @Deprecated
    ENABLE_CAMPAIGN_UI(true, "EnableCampaignUI", "Enable Campaign UI"), //
    @Deprecated
    USE_DNB_RTS_AND_MODELING(true, "UseDnbRtsAndModeling", "User DNB RTS and Modeling"), //
    @Deprecated
    ENABLE_LATTICE_MARKETO_CREDENTIAL_PAGE(true, "EnableLatticeMarketoCredentialPage", "Enable Lattice Marketo Credential Page"), //
    @Deprecated
    ENABLE_DATA_PROFILING_V2(true, "EnableDataProfilingV2", "Enable Data Profiling Version 2"), //
    @Deprecated
    ENABLE_FUZZY_MATCH(true, "EnableFuzzyMatch", "Enable Fuzzy Match"), //
    @Deprecated
    BYPASS_DNB_CACHE(true, "BypassDnbCache", "Bypass DnB Cache"), //
    @Deprecated
    ENABLE_CDL(true, "EnableCdl", "Enable Customer Data Lake"); //

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
