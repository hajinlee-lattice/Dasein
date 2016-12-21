package com.latticeengines.camille.exposed.paths;

import java.util.Arrays;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.metadata.ArtifactType;

public final class PathBuilder {
    public static Path buildPodsPath() {
        return new Path(Arrays.asList(PathConstants.PODS));
    }

    public static Path buildPodPath(String podId) {
        return new Path(PathConstants.PODS, podId);
    }

    public static Path buildPodDivisionPath(String podId, String division) {
        return new Path(PathConstants.PODS, podId, PathConstants.DIVISION, division);
    }

    public static Path buildServicesPath(String podId) {
        return new Path(PathConstants.PODS, podId, PathConstants.SERVICES);
    }

    public static Path buildServicePath(String podId, String serviceName) {
        return new Path(PathConstants.PODS, podId, PathConstants.SERVICES, serviceName);
    }

    public static Path buildServiceDefaultConfigPath(String podId, String serviceName) {
        return new Path(PathConstants.PODS, podId, PathConstants.DEFAULTCONFIG_NODE, serviceName);
    }

    public static Path buildServiceConfigSchemaPath(String podId, String serviceName) {
        return new Path(PathConstants.PODS, podId, PathConstants.CONFIGSCHEMA_NODE, serviceName);
    }

    public static Path buildContractsPath(String podId) {
        return new Path(PathConstants.PODS, podId, PathConstants.CONTRACTS);
    }

    public static Path buildContractPath(String podId, String contractId) {
        return new Path(PathConstants.PODS, podId, PathConstants.CONTRACTS, contractId);
    }

    public static Path buildTenantsPath(String podId, String contractId) {
        return new Path(PathConstants.PODS, podId, PathConstants.CONTRACTS, contractId, PathConstants.TENANTS);
    }

    public static Path buildTenantPath(String podId, String contractId, String tenantId) {
        return new Path(PathConstants.PODS, podId, PathConstants.CONTRACTS, contractId, PathConstants.TENANTS,
                tenantId);
    }

    public static Path buildCustomerSpacesPath(String podId, String contractId, String tenantId) {
        return new Path(PathConstants.PODS, podId, PathConstants.CONTRACTS, contractId, PathConstants.TENANTS, tenantId,
                PathConstants.SPACES);
    }

    public static Path buildCustomerSpacePath(String podId, String contractId, String tenantId, String spaceId) {
        return new Path(PathConstants.PODS, podId, PathConstants.CONTRACTS, contractId, PathConstants.TENANTS, tenantId,
                PathConstants.SPACES, spaceId);
    }

    public static Path buildDataTablePath(String podId, CustomerSpace space) {
        return buildCustomerSpacePath(podId, space).append(PathConstants.DATA).append(PathConstants.TABLES);
    }

    public static Path buildDataFilePath(String podId, CustomerSpace space) {
        return buildCustomerSpacePath(podId, space).append(PathConstants.DATA).append(PathConstants.FILES);
    }

    public static Path buildDataFileExportPath(String podId, CustomerSpace space) {
        return buildCustomerSpacePath(podId, space).append(PathConstants.DATA).append(PathConstants.FILES)
                .append(PathConstants.EXPORTS);
    }

    public static Path buildMetadataPath(String podId, CustomerSpace space) {
        return buildCustomerSpacePath(podId, space).append(PathConstants.METADATA);
    }

    public static Path buildMetadataPathForArtifactType(String podId, CustomerSpace space, String module,
            ArtifactType artifactType) {
        return buildMetadataPath(podId, space).append(module).append(artifactType.getPathToken());
    }

    public static Path buildCustomerSpacePath(String podId, CustomerSpace space) {
        return buildCustomerSpacePath(podId, space.getContractId(), space.getTenantId(), space.getSpaceId());
    }

    public static Path buildCustomerSpaceServicesPath(String podId, CustomerSpace space) {
        return buildCustomerSpaceServicesPath(podId, space.getContractId(), space.getTenantId(), space.getSpaceId());
    }

    public static Path buildCustomerSpaceServicesPath(String podId, String contractId, String tenantId,
            String spaceId) {
        return new Path(PathConstants.PODS, podId, PathConstants.CONTRACTS, contractId, PathConstants.TENANTS, tenantId,
                PathConstants.SPACES, spaceId, PathConstants.SERVICES);
    }

    public static Path buildCustomerSpaceServicePath(String podId, CustomerSpace space, String serviceName) {
        return buildCustomerSpaceServicePath(podId, space.getContractId(), space.getTenantId(), space.getSpaceId(),
                serviceName);
    }

    public static Path buildCustomerSpaceServicePath(String podId, String contractId, String tenantId, String spaceId,
            String serviceName) {
        return new Path(PathConstants.PODS, podId, PathConstants.CONTRACTS, contractId, PathConstants.TENANTS, tenantId,
                PathConstants.SPACES, spaceId, PathConstants.SERVICES, serviceName);
    }

    public static Path buildDataInterfacePath(String podId, String interfaceName, Long version, String contractId,
            String tenantId, String spaceId) {
        return new Path(PathConstants.PODS, podId, PathConstants.INTERFACES, PathConstants.DATA, interfaceName,
                contractId, tenantId, spaceId);
    }

    public static Path buildMessageQueuePath(String podId, String division, String queueName) {
        return new Path(PathConstants.PODS, podId, PathConstants.INTERFACES, PathConstants.DIVISION, division,
                PathConstants.QUEUES, queueName);
    }

    public static Path buildMessageQueuePath(String podId, String queueName) {
        return new Path(PathConstants.PODS, podId, PathConstants.INTERFACES, PathConstants.QUEUES, queueName);
    }

    public static Path buildModelingServicePath(String contractId, String tenantId, String spaceId) {
        return new Path(String.format("/user/s-analytics/customers/%s.%s.%s", contractId, tenantId, spaceId));
    }

    public static Path buildScoringServicePath(String contractId, String tenantId, String spaceId) {
        return new Path(String.format("/user/s-analytics/customers/%s.%s.%s/scoring", contractId, tenantId, spaceId));
    }

    public static Path buildFeatureFlagDefinitionPath(String podId) {
        return new Path(PathConstants.PODS, podId, PathConstants.FEATURE_FLAGS_DEFINITIONS_FILE);
    }

    public static Path buildFeatureFlagPath(String podId, CustomerSpace space) {
        return new Path(PathConstants.PODS, podId, PathConstants.CONTRACTS, space.getContractId(),
                PathConstants.TENANTS, space.getTenantId(), PathConstants.SPACES, space.getSpaceId(),
                PathConstants.FEATURE_FLAGS_FILE);
    }
}
