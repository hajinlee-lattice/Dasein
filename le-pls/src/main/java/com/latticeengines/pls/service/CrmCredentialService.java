package com.latticeengines.pls.service;

import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.pls.CrmCredential;

public interface CrmCredentialService {

    CrmCredential verifyCredential(String crmType, String tenantId, Boolean isProduction, CrmCredential crmCredential);

    CrmCredential getCredential(String crmType, String tenantId, Boolean isProduction);

    void removeCredentials(String crmType, String tenantId, Boolean isProduction);

    boolean useEaiToValidate(FeatureFlagValueMap flags);

}
