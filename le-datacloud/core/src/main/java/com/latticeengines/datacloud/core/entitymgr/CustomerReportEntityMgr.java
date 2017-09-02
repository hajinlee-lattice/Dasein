package com.latticeengines.datacloud.core.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.datacloud.customer.CustomerReport;

public interface CustomerReportEntityMgr extends BaseEntityMgr<CustomerReport> {

    CustomerReport findById(String id);
}
