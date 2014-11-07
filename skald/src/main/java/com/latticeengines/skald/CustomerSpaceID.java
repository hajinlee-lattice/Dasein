package com.latticeengines.skald;

public class CustomerSpaceID {
    public String contractID;

    public String tenantID;

    public String spaceID;

    @Override
    public String toString() {
        return contractID + "." + tenantID + "." + spaceID;
    }
}
