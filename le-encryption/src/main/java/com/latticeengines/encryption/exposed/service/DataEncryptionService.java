package com.latticeengines.encryption.exposed.service;

import java.util.List;

import com.latticeengines.domain.exposed.camille.CustomerSpace;

public interface DataEncryptionService {
    boolean isEncrypted(CustomerSpace space);

    @Deprecated
    void encrypt(CustomerSpace space);

    List<String> getEncryptedPaths(CustomerSpace space);

    void deleteKey(CustomerSpace space);
}
