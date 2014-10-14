package com.latticeengines.domain.exposed.camille;

import java.io.Serializable;

// TODO Eventual location for metadata mechanism
public class DocumentMetadata implements Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;
        if (other == null)
            return false;
        if (getClass() != other.getClass())
            return false;

        // TODO
        return true;
    }

    @Override
    public int hashCode() {
        // TODO
        return 1;
    }
}
