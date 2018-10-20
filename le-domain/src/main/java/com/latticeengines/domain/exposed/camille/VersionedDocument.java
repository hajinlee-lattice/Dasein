package com.latticeengines.domain.exposed.camille;

import com.fasterxml.jackson.annotation.JsonIgnore;

public abstract class VersionedDocument {

    @JsonIgnore
    private int version;

    public VersionedDocument() {
        version = -1;
    }

    public VersionedDocument(int version) {
        this.version = version;
    }

    public boolean documentVersionSpecified() {
        return version >= 0;
    }

    @JsonIgnore
    public int getDocumentVersion() {
        return version;
    }

    @JsonIgnore
    public void setDocumentVersion(int version) {
        if (version < -1) {
            throw new IllegalArgumentException(
                    "Version must either be -1 (unspecified), or greater or equal to 0");
        }
        this.version = version;
    }
}
