package com.latticeengines.matchapi.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.entitymgr.CustomerReportEntityMgr;
import com.latticeengines.domain.exposed.datacloud.customer.CustomerReport;
import com.latticeengines.matchapi.service.CustomerReportService;
@Component("customerReportService")
public class CustomerReportServiceImpl implements CustomerReportService {

    @Autowired
    private CustomerReportEntityMgr customerReportEntityMgr;
    @Override
    public void saveReport(CustomerReport customerReport) {
        customerReportEntityMgr.create(customerReport);
    }
    @Override
    public CustomerReport findById(String id) {
        return customerReportEntityMgr.findById(id);
    }

}
