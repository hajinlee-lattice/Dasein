package com.latticeengines.domain.exposed.auth;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class OneLoginExternalSession extends GlobalAuthExternalSession {

    public static final String TYPE = "OneLogin";

    @JsonProperty("NameId")
    private String nameId;

    @JsonProperty("NameIdFormat")
    private String nameIdFormat;

    @JsonProperty("SessionIndex")
    private String sessionIndex;

    @JsonProperty("NameIdNameQualifier")
    private String nameIdNameQualifier;

    @JsonProperty("NameIdSPNameQualifier")
    private String nameIdSPNameQualifier;

    @JsonProperty("Profile")
    private String profile;

    @JsonProperty("Attributes")
    private Map<String, List<String>> attributes;

    @Override
    @JsonProperty("Type")
    public String getType() {
        return TYPE;
    }

    public String getNameId() {
        return nameId;
    }

    public void setNameId(String nameId) {
        this.nameId = nameId;
    }

    public String getNameIdFormat() {
        return nameIdFormat;
    }

    public void setNameIdFormat(String nameIdFormat) {
        this.nameIdFormat = nameIdFormat;
    }

    public String getSessionIndex() {
        return sessionIndex;
    }

    public void setSessionIndex(String sessionIndex) {
        this.sessionIndex = sessionIndex;
    }

    public String getNameIdNameQualifier() {
        return nameIdNameQualifier;
    }

    public void setNameIdNameQualifier(String nameIdNameQualifier) {
        this.nameIdNameQualifier = nameIdNameQualifier;
    }

    public String getNameIdSPNameQualifier() {
        return nameIdSPNameQualifier;
    }

    public void setNameIdSPNameQualifier(String nameIdSPNameQualifier) {
        this.nameIdSPNameQualifier = nameIdSPNameQualifier;
    }

    public Map<String, List<String>> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, List<String>> attributes) {
        this.attributes = attributes;
    }

    public String getProfile() {
        return profile;
    }

    public void setProfile(String profile) {
        this.profile = profile;
    }

}
