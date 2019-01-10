package com.latticeengines.domain.exposed.datacloud.match;

import java.util.ArrayList;
import java.util.List;

public class EntityMatchKeyRecord {

    private String parsedDomain;
    private Boolean isPublicDomain = false;
    private boolean matchEvenIsPublicDomain;
    private String parsedDuns;
    private NameLocation parsedNameLocation;
    private String parsedEmail;

    private String origDomain;
    private NameLocation origNameLocation;
    private String origDuns;
    private String origEmail;

    // TODO(jwinter): Fix handling of error messages which should be passed using the actor system.
    private Boolean failed = false;
    private List<String> errorMessages;

    public String getParsedDomain() {
        return parsedDomain;
    }

    public void setParsedDomain(String parsedDomain) {
        this.parsedDomain = parsedDomain;
    }

    public Boolean isPublicDomain() {
        return isPublicDomain;
    }

    public void setPublicDomain(Boolean isPublicDomain) {
        this.isPublicDomain = isPublicDomain;
    }

    public boolean isMatchEvenIsPublicDomain() {
        return matchEvenIsPublicDomain;
    }

    public void setMatchEvenIsPublicDomain(boolean matchEvenIsPublicDomain) {
        this.matchEvenIsPublicDomain = matchEvenIsPublicDomain;
    }

    public String getParsedDuns() {
        return parsedDuns;
    }

    public void setParsedDuns(String parsedDuns) {
        this.parsedDuns = parsedDuns;
    }

    public NameLocation getParsedNameLocation() {
        return parsedNameLocation;
    }

    public void setParsedNameLocation(NameLocation parsedNameLocation) {
        this.parsedNameLocation = parsedNameLocation;
    }

    public String getParsedEmail() {
        return parsedEmail;
    }

    public void setParsedEmail(String parsedEmail) {
        this.parsedEmail = parsedEmail;
    }

    public String getOrigDomain() {
        return origDomain;
    }

    public void setOrigDomain(String origDomain) {
        this.origDomain = origDomain;
    }

    public NameLocation getOrigNameLocation() {
        return origNameLocation;
    }

    public void setOrigNameLocation(NameLocation origNameLocation) {
        this.origNameLocation = origNameLocation;
    }

    public String getOrigDuns() {
        return origDuns;
    }

    public void setOrigDuns(String origDuns) {
        this.origDuns = origDuns;
    }

    public String getOrigEmail() {
        return origEmail;
    }

    public void setOrigEmail(String origEmail) {
        this.origEmail = origEmail;
    }

    public Boolean isFailed() {
        return failed;
    }

    public void setFailed(Boolean failed) {
        this.failed = failed;
    }

    public List<String> getErrorMessages() {
        return errorMessages;
    }

    public void setErrorMessages(List<String> errorMessages) {
        this.errorMessages = errorMessages;
    }

    public void addErrorMessages(String errorMessage) {
        if (this.errorMessages == null) {
            this.errorMessages = new ArrayList<>();
        }
        this.errorMessages.add(errorMessage);
    }
}
