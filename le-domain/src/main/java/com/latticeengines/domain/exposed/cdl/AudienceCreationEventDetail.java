package com.latticeengines.domain.exposed.cdl;

public class AudienceCreationEventDetail extends EventDetail {

    public AudienceCreationEventDetail() {
        super("AudienceCreation");
    }

    private String audienceId;

    private String audienceName;

    public String getAudienceId() {
        return audienceId;
    }

    public void setAudienceId(String audienceId) {
        this.audienceId = audienceId;
    }

    public String getAudienceName() {
        return audienceName;
    }

    public void setAudienceName(String audienceName) {
        this.audienceName = audienceName;
    }

}
