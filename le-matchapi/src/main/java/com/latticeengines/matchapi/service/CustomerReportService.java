package com.latticeengines.matchapi.service;

import com.latticeengines.domain.exposed.datacloud.customer.CustomerReport;

public interface CustomerReportService {

    void saveReport(CustomerReport report);
    CustomerReport findById(String id);
}
