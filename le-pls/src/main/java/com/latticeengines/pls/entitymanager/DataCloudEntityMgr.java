package com.latticeengines.pls.entitymanager;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.datacloud.customer.CustomerReport;

public interface DataCloudEntityMgr extends BaseEntityMgr<CustomerReport> {

    CustomerReport findById(String id);

}
