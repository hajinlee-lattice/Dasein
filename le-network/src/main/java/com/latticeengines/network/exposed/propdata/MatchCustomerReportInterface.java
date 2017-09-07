package com.latticeengines.network.exposed.propdata;

import com.latticeengines.domain.exposed.datacloud.customer.CustomerReport;

public interface MatchCustomerReportInterface {

    Boolean matchCreateCustomerReport(CustomerReport report);
    CustomerReport matchFindById(String id);
}
