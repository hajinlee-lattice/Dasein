package com.latticeengines.domain.exposed.pls;

public enum NoteOrigin {

    NOTE("NOTE"), //
    REMODEL("REMODEL"), //
    MODELCREATED("MODEL CREATED");

    private String origin;

    NoteOrigin(String origin) {
        this.setOrigin(origin);
    }

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }
}
