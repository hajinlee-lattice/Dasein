package com.latticeengines.eai.entitymanager;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;

public interface EaiImportJobDetailEntityMgr extends BaseEntityMgr<EaiImportJobDetail> {

    EaiImportJobDetail findByCollectionIdentifier(String identifier);

    EaiImportJobDetail findByApplicationId(String appId);
}
