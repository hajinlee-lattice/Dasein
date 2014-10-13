package com.latticeengines.domain.exposed.camille;

// TODO Eventual location for metadata mechanism
public class DocumentMetadata implements DeepCopyable<DocumentMetadata> {

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

    @Override
    public DocumentMetadata deepCopy() {
        return new DocumentMetadata();
    }
}
