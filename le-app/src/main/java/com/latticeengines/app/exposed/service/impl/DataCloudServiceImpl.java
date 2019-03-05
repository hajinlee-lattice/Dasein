package com.latticeengines.app.exposed.service.impl;

import java.util.Date;
import java.util.UUID;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.app.exposed.service.DataCloudService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.datacloud.customer.CustomerReport;
import com.latticeengines.domain.exposed.datacloud.customer.CustomerReportType;
import com.latticeengines.domain.exposed.datacloud.customer.IncorrectLookupReproduceDetail;
import com.latticeengines.domain.exposed.datacloud.customer.IncorrectMatchedAttributeReproduceDetail;
import com.latticeengines.domain.exposed.pls.IncorrectLookupReportRequest;
import com.latticeengines.domain.exposed.pls.IncorrectMatchedAttrReportRequest;
import com.latticeengines.proxy.exposed.matchapi.MatchCustomerReportProxy;

@Service("dataCloudService")
public class DataCloudServiceImpl implements DataCloudService {

    private static final Logger log = LoggerFactory.getLogger(DataCloudServiceImpl.class);

    private final MatchCustomerReportProxy matchCustomerReportProxy;

    @Inject
    public DataCloudServiceImpl(MatchCustomerReportProxy matchCustomerReportProxy) {
        this.matchCustomerReportProxy = matchCustomerReportProxy;
    }

    @Override
    public CustomerReport reportIncorrectLookup(IncorrectLookupReportRequest reportRequest) {
        log.info("Received customer report \n" + JsonUtils.pprint(reportRequest));

        CustomerReport customerReport = new CustomerReport();
        customerReport.setId(UUID.randomUUID().toString());

        customerReport.setType(CustomerReportType.LOOKUP);
        customerReport.setCreatedTime(new Date());
        customerReport.setComment(reportRequest.getComment());
        customerReport.setReportedByTenant(MultiTenantContext.getCustomerSpace().getTenantId());
        customerReport.setReportedByUser(MultiTenantContext.getEmailAddress());
        customerReport.setMatchLog(reportRequest.getMatchLog());
        customerReport.setSuggestedValue(reportRequest.getCorrectValue());
        IncorrectLookupReproduceDetail detail = new IncorrectLookupReproduceDetail();
        detail.setInputKeys(reportRequest.getInputKeys());
        detail.setMatchedKeys(reportRequest.getMatchedKeys());
        customerReport.setReproduceDetail(detail);

        matchCustomerReportProxy.matchCreateCustomerReport(customerReport);
        return customerReport;
    }

    @Override
    public CustomerReport reportIncorrectMatchedAttr(IncorrectMatchedAttrReportRequest reportRequest) {
        log.info("Received customer report \n" + JsonUtils.pprint(reportRequest));

        CustomerReport customerReport = new CustomerReport();
        customerReport.setId(UUID.randomUUID().toString());

        customerReport.setType(CustomerReportType.MATCHEDATTRIBUTE);
        customerReport.setCreatedTime(new Date());
        customerReport.setComment(reportRequest.getComment());
        customerReport.setReportedByTenant(MultiTenantContext.getCustomerSpace().getTenantId());
        customerReport.setReportedByUser(MultiTenantContext.getEmailAddress());
        customerReport.setMatchLog(reportRequest.getMatchLog());
        customerReport.setSuggestedValue(reportRequest.getCorrectValue());
        IncorrectMatchedAttributeReproduceDetail detail = new IncorrectMatchedAttributeReproduceDetail();
        detail.setInputKeys(reportRequest.getInputKeys());
        detail.setMatchedKeys(reportRequest.getMatchedKeys());
        detail.setAttribute(reportRequest.getAttribute());
        detail.setMatchedValue(reportRequest.getMatchedValue());
        customerReport.setIncorrectAttribute(reportRequest.getAttribute());
        customerReport.setReproduceDetail(detail);

        matchCustomerReportProxy.matchCreateCustomerReport(customerReport);
        return customerReport;
    }

    @Override
    public CustomerReport findById(String id) {
        return matchCustomerReportProxy.matchFindById(id);
    }

}
