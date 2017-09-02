package com.latticeengines.datacloud.core.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.datacloud.customer.CustomerReport;

public interface CustomerReportDao extends BaseDao<CustomerReport> {

    CustomerReport findById(String id);
}
