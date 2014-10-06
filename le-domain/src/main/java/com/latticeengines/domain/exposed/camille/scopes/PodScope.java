package com.latticeengines.domain.exposed.camille.scopes;

public class PodScope extends ConfigurationScope {
    private String podID;

    public PodScope(String podID) {
        this.podID = podID;
    }

    public String getPodID() {
        return podID;
    }

    public void setPodID(String podID) {
        this.podID = podID;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((podID == null) ? 0 : podID.hashCode());
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
        PodScope other = (PodScope) obj;
        if (podID == null) {
            if (other.podID != null)
                return false;
        } else if (!podID.equals(other.podID))
            return false;
        return true;
    }

    @Override
    public Type getType() {
        return Type.POD;
    }
}
