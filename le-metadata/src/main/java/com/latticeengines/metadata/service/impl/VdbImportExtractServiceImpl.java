package com.latticeengines.metadata.service.impl;

import com.latticeengines.domain.exposed.metadata.VdbImportExtract;
import com.latticeengines.metadata.entitymgr.VdbImportExtractEntityMgr;
import com.latticeengines.metadata.service.VdbImportExtractService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component("vdbImportExtractService")
public class VdbImportExtractServiceImpl implements VdbImportExtractService {

    @Autowired
    private VdbImportExtractEntityMgr vdbImportExtractEntityMgr;

    @Override
    public VdbImportExtract getVdbImportExtract(String customerSpace, String extractIdentifier) {
        return vdbImportExtractEntityMgr.findByExtractIdentifier(extractIdentifier);
    }

    @Override
    public boolean updateVdbImportExtract(String customerSpace, VdbImportExtract importExtract) {
        vdbImportExtractEntityMgr.update(importExtract);
        return true;
    }

    @Override
    public boolean createVdbImportExtract(String customerSpace, VdbImportExtract importExtract) {
        vdbImportExtractEntityMgr.create(importExtract);
        return true;
    }

    @Override
    public boolean existVdbImportExtract(String customerSpace, String extractIdentifier) {
        return vdbImportExtractEntityMgr.findByExtractIdentifier(extractIdentifier) != null;
    }
}
