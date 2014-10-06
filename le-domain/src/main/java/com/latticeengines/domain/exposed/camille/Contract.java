package com.latticeengines.domain.exposed.camille;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Contract {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    private String contractId;

    public Contract() {
    }

    public Contract(String contractId) {
        this.contractId = contractId;
    }

    public String getContractId() {
        return contractId;
    }

    public void setContractId(String contractId) {
        this.contractId = contractId;
    }

    @Override
    public String toString() {
        return "Contract [contractId=" + contractId + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((contractId == null) ? 0 : contractId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof Contract))
            return false;
        Contract other = (Contract) obj;
        if (contractId == null) {
            if (other.contractId != null)
                return false;
        } else if (!contractId.equals(other.contractId))
            return false;
        return true;
    }
}
