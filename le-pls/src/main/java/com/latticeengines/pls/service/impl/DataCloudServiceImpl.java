package com.latticeengines.pls.service.impl;

import java.util.Date;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.customer.CustomerReport;
import com.latticeengines.domain.exposed.datacloud.customer.CustomerReportType;
import com.latticeengines.domain.exposed.datacloud.customer.IncorrectLookupReproduceDetail;
import com.latticeengines.domain.exposed.datacloud.customer.IncorrectMatchedAttributeReproduceDetail;
import com.latticeengines.domain.exposed.pls.IncorrectLookupReportRequest;
import com.latticeengines.domain.exposed.pls.IncorrectMatchedAttrReportRequest;
import com.latticeengines.pls.entitymanager.DataCloudEntityMgr;
import com.latticeengines.pls.service.DataCloudService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Service("dataCloudService")
public class DataCloudServiceImpl implements DataCloudService {

    private static final Logger log = LoggerFactory.getLogger(DataCloudServiceImpl.class);
    @Autowired
    private DataCloudEntityMgr dataCloudEntityMgr;
    @Override
    public CustomerReport reportIncorrectLookup(IncorrectLookupReportRequest reportRequest) {
        log.info("Received customer report \n" + JsonUtils.pprint(reportRequest));

        CustomerReport customerReport = new CustomerReport();
        customerReport.setId(UUID.randomUUID().toString());

        customerReport.setType(CustomerReportType.LOOkUP);
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

        dataCloudEntityMgr.create(customerReport);
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
        customerReport.setReproduceDetail(detail);

        dataCloudEntityMgr.create(customerReport);
        return customerReport;
    }

    @Override
    public CustomerReport findById(String id) {
        return dataCloudEntityMgr.findById(id);
    }

}
