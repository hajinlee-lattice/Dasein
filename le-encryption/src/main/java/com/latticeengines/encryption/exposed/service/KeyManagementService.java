package com.latticeengines.encryption.exposed.service;

import com.latticeengines.domain.exposed.camille.CustomerSpace;

public interface KeyManagementService {
    String getKeyName(CustomerSpace space);

    void createKey(CustomerSpace space);

    void deleteKey(CustomerSpace space);
}
