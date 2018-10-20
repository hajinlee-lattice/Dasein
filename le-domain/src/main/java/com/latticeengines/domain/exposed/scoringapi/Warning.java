package com.latticeengines.domain.exposed.scoringapi;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Warning {

    private static final int MAX_ALLOWED_CHARS = 1000;

    @JsonProperty("warning")
    private String warning;

    @JsonProperty("warning_description")
    private String description;

    @JsonIgnore
    private WarningCode code;

    public Warning() {

    }

    public Warning(WarningCode code, String[] params) {
        this.code = code;
        this.warning = code.getExternalCode();
        this.description = buildDescription(code, params);
    }

    public static String buildDescription(WarningCode code, String[] params) {
        String description = code.getDescription();

        for (int i = 0; i < params.length; i++) {
            String param = params[i];
            if (param != null) {
                // we need to escape $ from param otherwise it interfere with
                // replace logic
                param = param.replace("$", "\\$");
            }
            description = description.replaceAll("\\{" + i + "\\}", param);
        }
        return description;
    }

    public String getWarning() {
        return warning;
    }

    public void setWarning(String warning) {
        this.warning = warning;
    }

    public String getDescription() {
        if (Math.min(description.length(), MAX_ALLOWED_CHARS) == description.length()) {
            return description;
        }
        // if description is too big then trim it
        return String.format("%s ...", description.substring(0, MAX_ALLOWED_CHARS));
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public WarningCode getCode() {
        return code;
    }
}
