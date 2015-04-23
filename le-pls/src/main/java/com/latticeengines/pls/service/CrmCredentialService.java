package com.latticeengines.pls.service;

import com.latticeengines.domain.exposed.pls.CrmCredential;

public interface CrmCredentialService {

    public static final String CRM_SFDC = "sfdc";
    public static final String CRM_MARKETO = "marketo";
    public static final String CRM_ELOQUA = "eloqua";
    
    CrmCredential verifyCredential(String crmType, String tenantId, String contractId, CrmCredential crmCredential);

    CrmCredential getCredential(String crmType, String tenantId, String contractId);

}
