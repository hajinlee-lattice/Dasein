package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SureShotUrls {

    private String credsUrl;

    private String scoringSettingsUrl;

    public SureShotUrls(String credsUrl, String scoringSettingsUrl) {
        this.credsUrl = credsUrl;
        this.scoringSettingsUrl = scoringSettingsUrl;
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
}
