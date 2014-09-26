package com.latticeengines.camille.paths;

import com.latticeengines.domain.exposed.camille.Path;

public final class PathBuilder {
    public static Path buildPodPath(String podID) {
        return new Path(DirectoryConstants.PODS, podID);
    }
    
    public static Path buildServicePath(String podID, String serviceName) {
        return new Path(DirectoryConstants.PODS, podID, DirectoryConstants.SERVICES, serviceName);
    }

    public static Path buildContractPath(String podID, String contractID) {
        return new Path(DirectoryConstants.PODS, podID, DirectoryConstants.CONTRACTS, contractID);
    }
    
    public static Path buildTenantPath(String podID, String contractID, String tenantID) {
        return new Path(
                DirectoryConstants.PODS, 
                podID, 
                DirectoryConstants.CONTRACTS, 
                contractID, 
                DirectoryConstants.TENANTS, 
                tenantID);
        
    }
    
    public static Path buildSpacesPath(String podID, String contractID, String tenantID, String spaceID) {
        return new Path(
                DirectoryConstants.PODS,
                podID,
                DirectoryConstants.CONTRACTS,
                contractID,
                DirectoryConstants.TENANTS,
                tenantID,
                DirectoryConstants.SPACES,
                spaceID);
    }
    
    public static Path buildTenantServicePath(String podID, String contractID, String tenantID, String spaceID, String serviceName) {
        return new Path(
                DirectoryConstants.PODS,
                podID,
                DirectoryConstants.CONTRACTS,
                contractID,
                DirectoryConstants.TENANTS,
                tenantID,
                DirectoryConstants.SPACES,
                spaceID,
                DirectoryConstants.SERVICES,
                serviceName);
    }
}
