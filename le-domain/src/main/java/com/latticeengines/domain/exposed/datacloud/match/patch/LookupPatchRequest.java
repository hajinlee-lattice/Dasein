package com.latticeengines.domain.exposed.datacloud.match.patch;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.event.Level;

/**
 * Request entity for DataCloud Lookup Patcher
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class LookupPatchRequest extends PatchRequest {
    private static final boolean DEFAULT_DRY_RUN_ENABLED = true;
    private static final Level DEFAULT_LOG_LEVEL = Level.INFO;

    @JsonProperty("DryRun")
    private boolean dryRun = DEFAULT_DRY_RUN_ENABLED;

    @JsonProperty("LogLevel")
    private Level logLevel = DEFAULT_LOG_LEVEL;

    public boolean isDryRun() {
        return dryRun;
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }

    public Level getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(Level logLevel) {
        this.logLevel = logLevel;
    }
}
