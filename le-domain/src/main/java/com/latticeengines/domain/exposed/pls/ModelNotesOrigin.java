package com.latticeengines.domain.exposed.pls;

public enum ModelNotesOrigin {

    NOTE("NOTE"),
    REMODEL("REMODEL"),
    MODELCREATED("MODEL CREATED");

    private ModelNotesOrigin(String origin) {
        this.setOrigin(origin);
    }

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }

    private String origin;
}
