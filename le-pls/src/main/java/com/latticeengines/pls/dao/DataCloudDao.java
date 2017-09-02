package com.latticeengines.pls.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.datacloud.customer.CustomerReport;

public interface DataCloudDao extends BaseDao<CustomerReport> {

    CustomerReport findById(String id);

}
