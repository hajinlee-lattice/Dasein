package com.latticeengines.eai.exposed.service;

import com.latticeengines.domain.exposed.source.SourceCredentialType;

public interface EaiCredentialValidationService {

    void validateCrmCredential(String customerSpace, SourceCredentialType sourceCredentialType);

    void validateCredential(String customerSpace, String crmType, SourceCredentialType sourceCredentialType);

}
