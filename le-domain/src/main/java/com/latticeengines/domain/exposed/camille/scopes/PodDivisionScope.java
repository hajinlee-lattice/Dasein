package com.latticeengines.domain.exposed.camille.scopes;

public class PodDivisionScope extends ConfigurationScope {

    public PodDivisionScope() {
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + getType().hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        return true;
    }

    @Override
    public Type getType() {
        return Type.POD_DIVISION;
    }
}
