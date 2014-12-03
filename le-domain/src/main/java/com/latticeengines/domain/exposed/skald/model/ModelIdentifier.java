package com.latticeengines.domain.exposed.skald.model;

public class ModelIdentifier {
    public ModelIdentifier(String name, int version) {
        this.name = name;
        this.version = version;
    }
    
    // Serialization Constructor.
    public ModelIdentifier() {
    }
    
    public String name;

    public int version;

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + version;
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
        ModelIdentifier other = (ModelIdentifier) obj;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (version != other.version)
            return false;
        return true;
    }
}
