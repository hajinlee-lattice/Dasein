package com.latticeengines.domain.exposed.admin;

import java.util.HashSet;
import java.util.Set;

import com.latticeengines.domain.exposed.pls.PlsFeatureFlag;

public enum LatticeFeatureFlag {

    ALPHA_FEATURE("AlphaFeature", "Allows alpha features"), //
    BETA_FEATURE("BetaFeature", "Allows beta features"), //
    PROTOTYPE_FEATURE("PrototypeFeature", "Allows prototype features"), //

    DANTE("Dante", "Dante"), //
    ALLOW_PIVOT_FILE("AllowPivotFile", "Allow pivot file"), //
    ENABLE_DATA_ENCRYPTION("EnableDataEncryption", "Enable data encryption"), //
    ENABLE_INTERNAL_ENRICHMENT_ATTRIBUTES("EnableInternalEnrichmentAttributes",
            "Enable Internal Enrichment Attributes"), //
    LATTICE_INSIGHTS("LatticeInsights", "Lattice Insights"), //
    LATTICE_MARKETO_SCORING("LatticeMarketoScoring", "Lattice Integration for Marketo Scoring"), //
    ENABLE_MATCH_DEBUG("EnableMatchDebug", "Enable Match Debug"), //
    ENABLE_ENTITY_MATCH("EnableEntityMatch", "Enable Entity Match"), //
    // TODO: To be retired after all the tenants are migrated to entity match
    ENABLE_ENTITY_MATCH_GA("EnableEntityMatchGA", "Enable Entity Match GA"), //
    ENABLE_APS_IMPUTATION("EnableApsImputation", "Enable APS null imputation with 0"), //
    ENABLE_LPI_PLAYMAKER("EnableLpiPlaymaker", "Enable LPI Playmaker"), //
    ALLOW_AUTO_SCHEDULE("AllowAutoSchedule", "Allow Auto Schedule"), //
    ENABLE_DATA_CLOUD_REFRESH_ACTIVITY("EnableDataCloudRefreshActivity", "Enable Data Cloud Refresh Activity"), //
    VDB_MIGRATION("VDBMigration", "VDB Migration"), //
    SCORE_EXTERNAL_FILE("ScoreExternalFile", "Score External File"), //
    ENABLE_FILE_IMPORT("EnableFileImport", "Enable File Import"), //
    ENABLE_CROSS_SELL_MODELING("EnableCrossSellModeling", "Enable Cross Sell Modeling"), //
    ENABLE_PRODUCT_PURCHASE_IMPORT("EnableProductPurchaseImport", "Enable Product Purchase Import"), //
    ENABLE_PRODUCT_BUNDLE_IMPORT("EnableProductBundleImport", "Enable Product Bundle Import"), //
    ENABLE_PRODUCT_HIERARCHY_IMPORT("EnableProductHierarchyImport", "Enable Product Hierarchy Import"), //
    ENABLE_EXTERNAL_INTEGRATION("EnableExternalIntegration", "Enable External Integration"), //
    PLAYBOOK_MODULE("PlaybookModule", "Playbook Module"), //
    LAUNCH_PLAY_TO_MAP_SYSTEM("LaunchPlayToMapSystem", "Launch Play to MAP System"), //
    AUTO_IMPORT_ON_INACTIVE("AutoImportOnInactive", "Run Auto Import on Inactive stack"), //
    IMPORT_WITHOUT_ID("ImportWithoutIds", "Allow import without id columns."), //
    ADVANCED_MODELING("AdvancedModeling", "Allow advanced modeling"), //
    MIGRATION_TENANT("MigrationTenant", "Allows features created only for playmaker migration tenants"), //
    ENABLE_FACEBOOK_INTEGRATION("EnableFacebookIntegration", "Enable Facebook integration"), //
    ENABLE_GOOGLE_INTEGRATION("EnableGoogleIntegration", "Enable Google integration"), //
    ENABLE_LIVERAMP_INTEGRATION("EnableLiveRampIntegration", "Enable LiveRamp integration"), //
    ENABLE_OUTREACH_INTEGRATION("EnableOutreachIntegration", "Enable Outreach integration"), //
    ENABLE_ACCOUNT360("EnableAccount360", "Enable Account360 features"), //
    ATTRIBUTE_TOGGLING("AttributeToggling", "Allow multi click behavior for Attributes during query building"), //
    ENABLE_ACXIOM("EnableAcxiom", "Enable Acxiom in ads platform integrations"), //
    ENABLE_IMPORT_V2("EnableImportV2", "Enable Import UI 2.0"), //
    SSVI_REPORT("SSVIReport", "SSVIReport"), //
    ENABLE_IMPORT_ERASE_BY_NULL("EnableImportEraseByNull", "enable using null in import to erase the previous value"),

    DCP_ENRICHMENT_LIBRARY("EnrichmentLibrary", "Enable Enrichment Library features"),

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
    ENABLE_LATTICE_MARKETO_CREDENTIAL_PAGE(true, "EnableLatticeMarketoCredentialPage",
            "Enable Lattice Marketo Credential Page"), //
    @Deprecated
    ENABLE_DATA_PROFILING_V2(true, "EnableDataProfilingV2", "Enable Data Profiling Version 2"), //
    @Deprecated
    ENABLE_FUZZY_MATCH(true, "EnableFuzzyMatch", "Enable Fuzzy Match"), //
    @Deprecated
    BYPASS_DNB_CACHE(true, "BypassDnbCache", "Bypass DnB Cache"), //
    @Deprecated
    ENABLE_CDL(true, "EnableCdl", "Enable Customer Data Lake"), //
    @Deprecated
    ENABLE_TARGET_SCORE_DERIVATION(true, "EnableTargetScoreDerivation", "Enable Target Score Derivation"), //
    @Deprecated
    ALWAYS_ON_CAMPAIGNS(true, "AlwaysOnCampaigns", "Allow updated Campagin Dashboard UI for Always On Campaigns"), //
    @Deprecated
    ENABLE_LINKEDIN_INTEGRATION(true, "EnableLinkedInIntegration", "Enable LinkedIn integration"), //
    @Deprecated
    ADVANCED_FILTERING(true, "AdvancedFiltering",
            "Enable Advanced filtering functionality for Segments, Models and Campaigns"), //
    @Deprecated
    ENABLE_DELTA_CALCULATION(true, "EnableDeltaCalculation", "Enable Delta Calculation"); //

    private static Set<String> names;

    static {
        names = new HashSet<>();
        for (PlsFeatureFlag flag : PlsFeatureFlag.values()) {
            names.add(flag.getName());
        }
    }

    private String name;
    private String documentation;
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

}
