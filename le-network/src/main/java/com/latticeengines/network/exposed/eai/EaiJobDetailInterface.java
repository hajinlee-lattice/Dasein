package com.latticeengines.network.exposed.eai;

import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;

public interface EaiJobDetailInterface {

    EaiImportJobDetail getImportJobDetail(String collectionIdentifier);

    void cancelImportJob(String collectionIdentifier);

}
