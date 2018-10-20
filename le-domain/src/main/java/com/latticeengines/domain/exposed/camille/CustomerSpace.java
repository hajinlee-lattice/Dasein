package com.latticeengines.domain.exposed.camille;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class CustomerSpace {
    public static final String BACKWARDS_COMPATIBLE_SPACE_ID = "Production";
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());
    private String contractId;
    private String tenantId;
    private String spaceId;

    // Serialization constructor.
    public CustomerSpace() {
    }

    public CustomerSpace(String contractId, String tenantId, String spaceId) {
        this.contractId = contractId.trim();
        this.tenantId = tenantId.trim();
        this.spaceId = spaceId.trim();
    }

    /**
     * Parse the specified 3-part or 1-part identifier into a CustomerSpace. The
     * identifier may be a Deployment ExternalID, such as DellAPJ, or a 3-part
     * identifier, such as Dell.APJ.Production.
     *
     * @param identifier
     * @return CustomerSpace
     */
    public static CustomerSpace parse(String identifier) {
        if (identifier == null) {
            throw new NullPointerException(
                    "Identifier parameter to CustomerSpace.parse may not be null");
        }

        String[] parts = identifier.split("\\.");
        if (parts.length != 3 && parts.length != 1) {
            throw new RuntimeException(String.format(
                    "Identifiers must either contain no dots (e.g., 'DellAPJ') or are in a 3-part format (e.g., 'Dell.APJ.Production').  Identifier %s is invalid",
                    identifier));
        }

        if (parts.length == 1) {
            log.debug(String.format(
                    "Using backwards-compatible conversion to extract Contract, Tenant, and Space IDs from Deployment External ID %s.  Assuming %s is %s.",
                    identifier, identifier, String.format("%s.%s.%s", identifier, identifier,
                            BACKWARDS_COMPATIBLE_SPACE_ID)));
            return new CustomerSpace(identifier, identifier, BACKWARDS_COMPATIBLE_SPACE_ID);
        }

        return new CustomerSpace(parts[0], parts[1], parts[2]);
    }

    public static String shortenCustomerSpace(String customerSpace) {
        return CustomerSpace.parse(customerSpace).getTenantId();
    }

    /**
     * @return the 1-part identifier of this customer space, if possible.
     *         Otherwise, returns a 3-part identifier.
     */
    @JsonIgnore
    public String getBackwardsCompatibleIdentifier() {
        if (getSpaceId().equals(BACKWARDS_COMPATIBLE_SPACE_ID)
                && getContractId().equals(getTenantId())) {
            return getContractId();
        }
        return toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((contractId == null) ? 0 : contractId.hashCode());
        result = prime * result + ((spaceId == null) ? 0 : spaceId.hashCode());
        result = prime * result + ((tenantId == null) ? 0 : tenantId.hashCode());
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
        CustomerSpace other = (CustomerSpace) obj;
        if (contractId == null) {
            if (other.contractId != null)
                return false;
        } else if (!contractId.equals(other.contractId))
            return false;
        if (spaceId == null) {
            if (other.spaceId != null)
                return false;
        } else if (!spaceId.equals(other.spaceId))
            return false;
        if (tenantId == null) {
            if (other.tenantId != null)
                return false;
        } else if (!tenantId.equals(other.tenantId))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return String.format("%s.%s.%s", contractId, tenantId, spaceId);
    }

    public String getContractId() {
        return contractId;
    }

    public void setContractId(String contractId) {
        this.contractId = contractId;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getSpaceId() {
        return spaceId;
    }

    public void setSpaceId(String spaceId) {
        this.spaceId = spaceId;
    }

}
