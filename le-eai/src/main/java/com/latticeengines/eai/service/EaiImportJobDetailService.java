package com.latticeengines.eai.service;

import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;

public interface EaiImportJobDetailService {

    EaiImportJobDetail getImportJobDetail(String collectionIdentifier);

    boolean updateImportJobDetail(EaiImportJobDetail eaiImportJobDetail);

    void createImportJobDetail(EaiImportJobDetail eaiImportJobDetail);

    void deleteImportJobDetail(EaiImportJobDetail eaiImportJobDetail);

    void cancelImportJob(String collectionIdentifier);
}
