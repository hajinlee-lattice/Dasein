package com.latticeengines.domain.exposed.camille;

// TODO Eventual location for metadata mechanism
public class DocumentMetadata {
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof DocumentMetadata)) {
            return false;
        }
        // TODO
        return true;
    }
    
    @Override
    public int hashCode() {
        // TODO
        return 1;
    }
}
