package com.latticeengines.remote.exposed.service;

import com.latticeengines.domain.exposed.pls.CrmCredential;

public interface CrmCredentialZKService {

    void writeToZooKeeper(String crmType, String tenantId, Boolean isProduction, CrmCredential crmCredential,
            boolean isWriteCustomerSpace);

    CrmCredential getCredential(String crmType, String tenantId, Boolean isProduction);

    void removeCredentials(String crmType, String tenantId, Boolean isProduction);

}
