package com.latticeengines.proxy.exposed.matchapi;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.datacloud.customer.CustomerReport;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("matchCustomerReportProxy")
public class MatchCustomerReportProxy extends BaseRestApiProxy {

    public MatchCustomerReportProxy() {
        super(PropertyUtils.getProperty("common.matchapi.url"), "/match/customerreports");
    }

    public Boolean matchCreateCustomerReport(CustomerReport report) {
        String url = constructUrl("/");
        return post("createCustomerReport", url, report, Boolean.class);
    }

    public CustomerReport matchFindById(String id) {
        String url = constructUrl("/{id}", id);
        return get("getById", url, CustomerReport.class);
    }


}
