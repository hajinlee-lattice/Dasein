package com.latticeengines.camille.paths;

import java.util.Arrays;

import com.latticeengines.domain.exposed.camille.Path;

public final class PathBuilder {
    public static Path buildPodsPath() {
        return new Path(Arrays.asList(PathConstants.PODS));
    }

    public static Path buildPodPath(String podID) {
        return new Path(PathConstants.PODS, podID);
    }

    public static Path buildServicePath(String podID, String serviceName) {
        return new Path(PathConstants.PODS, podID, PathConstants.SERVICES, serviceName);
    }

    public static Path buildContractsPath(String podID) {
        return new Path(PathConstants.PODS, podID, PathConstants.CONTRACTS);
    }

    public static Path buildContractPath(String podID, String contractID) {
        return new Path(PathConstants.PODS, podID, PathConstants.CONTRACTS, contractID);
    }

    public static Path buildTenantsPath(String podID, String contractID) {
        return new Path(PathConstants.PODS, podID, PathConstants.CONTRACTS, contractID, PathConstants.TENANTS);
    }

    public static Path buildTenantPath(String podID, String contractID, String tenantID) {
        return new Path(PathConstants.PODS, podID, PathConstants.CONTRACTS, contractID, PathConstants.TENANTS, tenantID);
    }

    public static Path buildCustomerSpacePath(String podID, String contractID, String tenantID, String spaceID) {
        return new Path(PathConstants.PODS, podID, PathConstants.CONTRACTS, contractID, PathConstants.TENANTS,
                tenantID, PathConstants.SPACES, spaceID);
    }

    public static Path buildCustomerSpaceServicePath(String podID, String contractID, String tenantID, String spaceID,
            String serviceName) {
        return new Path(PathConstants.PODS, podID, PathConstants.CONTRACTS, contractID, PathConstants.TENANTS,
                tenantID, PathConstants.SPACES, spaceID, PathConstants.SERVICES, serviceName);
    }
}
