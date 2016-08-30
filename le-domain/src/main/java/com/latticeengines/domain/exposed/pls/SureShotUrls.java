package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SureShotUrls {

    private String credsUrl;

    private String scoringSettingsUrl;

    private String enrichmentSettingsUrl;

    public SureShotUrls(String credsUrl, String scoringSettingsUrl, String enrichmentSettingsUrl) {
        this.credsUrl = credsUrl;
        this.scoringSettingsUrl = scoringSettingsUrl;
        this.enrichmentSettingsUrl = enrichmentSettingsUrl;
    }

    @JsonProperty("creds_url")
    public String getCredsUrl() {
        return this.credsUrl;
    }

    @JsonProperty("creds_url")
    public void setCredsUrl(String credsUrl) {
        this.credsUrl = credsUrl;
    }

    @JsonProperty("scoring_settings_url")
    public String getScoringSettingsUrl() {
        return this.scoringSettingsUrl;
    }

    @JsonProperty("scoring_settings_url")
    public void setScoringSettingsUrl(String scoringSettingsUrl) {
        this.scoringSettingsUrl = scoringSettingsUrl;
    }

    @JsonProperty("enrichment_settings_url")
    public String getEnrichmentSettingsUrl() {
        return this.enrichmentSettingsUrl;
    }

    @JsonProperty("enrichment_settings_url")
    public void setEnrichmentSettingsUrl(String enrichmentSettingsUrl) {
        this.enrichmentSettingsUrl = enrichmentSettingsUrl;
    }
}
