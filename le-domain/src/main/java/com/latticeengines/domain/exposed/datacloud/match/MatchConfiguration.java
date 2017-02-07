package com.latticeengines.domain.exposed.datacloud.match;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MatchConfiguration {

    @JsonProperty("TimeOut")
    private Long timeout;

    // ====================
    // boolean flags
    // ====================

    @JsonProperty("requestSource")
    private String requestSource;   // scoring|modeling

    // allowed to be null;
    @JsonProperty("UseRemoteDnB")
    private Boolean useRemoteDnB;

    @JsonProperty("DisableDunsValidation")
    private boolean disableDunsValidation;

    public Long getTimeout() {
        return timeout;
    }

    private void setTimeout(Long timeout) {
        this.timeout = timeout;
    }

    public Boolean getUseRemoteDnB() {
        return useRemoteDnB;
    }

    private void setUseRemoteDnB(Boolean useRemoteDnB) {
        this.useRemoteDnB = useRemoteDnB;
    }
    
    public String getRequestSource() {
        return requestSource;
    }

    private void setRequestSource(String requestSource) {
        this.requestSource = requestSource;
    }

    public boolean getDisableDunsValidation() {
        return disableDunsValidation;
    }

    private void setDisableDunsValidation(boolean disableDunsValidation) {
        this.disableDunsValidation = disableDunsValidation;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder from(MatchConfiguration configuration) {
        return builder() //
                .timeout(configuration.getTimeout()) //
                .useRemoteDnB(configuration.getUseRemoteDnB())
                .disableDunsValidation(configuration.getDisableDunsValidation());
    }

    public static class Builder {

        private Long timeout;
        private Boolean useRemoteDnB;
        private String requestSource;
        private boolean disableDunsValidation;
        
        private Builder() {
        }

        public Builder timeout(Long timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder useRemoteDnB(Boolean useRemoteDnB) {
            this.useRemoteDnB = useRemoteDnB;
            return this;
        }

        public Builder requestSource(String requestSource) {
            this.requestSource = requestSource;
            return this;
        }

        public Builder disableDunsValidation(boolean disableDunsValidation) {
            this.disableDunsValidation = disableDunsValidation;
            return this;
        }

        public MatchConfiguration build() {
            MatchConfiguration configuration = new MatchConfiguration();
            configuration.setTimeout(timeout);
            configuration.setUseRemoteDnB(useRemoteDnB);
            configuration.setRequestSource(requestSource);
            configuration.setDisableDunsValidation(disableDunsValidation);
            return configuration;
        }

    }
}
