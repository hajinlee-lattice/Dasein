package com.latticeengines.metadata.service;

import com.latticeengines.domain.exposed.metadata.VdbImportExtract;

public interface VdbImportExtractService {

    VdbImportExtract getVdbImportExtract(String customerSpace, String extractIdentifier);

    boolean updateVdbImportExtract(String customerSpace, VdbImportExtract importExtract);

    boolean createVdbImportExtract(String customerSpace, VdbImportExtract importExtract);

    boolean existVdbImportExtract(String customerSpace, String extractIdentifier);
}
