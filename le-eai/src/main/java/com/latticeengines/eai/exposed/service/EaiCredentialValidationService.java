package com.latticeengines.eai.exposed.service;

public interface EaiCredentialValidationService {

    void validateCrmCredential(String customerSpace);

    void validateCredential(String customerSpace, String crmType);

}
