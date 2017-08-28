package com.latticeengines.pls.service.impl;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.customer.CustomerReport;
import com.latticeengines.domain.exposed.datacloud.customer.CustomerReportType;
import com.latticeengines.domain.exposed.pls.IncorrectLookupReportRequest;
import com.latticeengines.domain.exposed.pls.IncorrectMatchedAttrReportRequest;
import com.latticeengines.pls.service.DataCloudService;

@Service("dataCloudService")
public class DataCloudServiceImpl implements DataCloudService {

    private static final Logger log = LoggerFactory.getLogger(DataCloudServiceImpl.class);

    @Override
    public CustomerReport reportIncorrectLookup(IncorrectLookupReportRequest reportRequest) {
        log.info("Received customer report \n" + JsonUtils.pprint(reportRequest));

        //TODO: change to call matchapi
        CustomerReport customerReport = new CustomerReport();
        customerReport.setId(UUID.randomUUID().toString());

        customerReport.setType(CustomerReportType.LOOkUP);

        return customerReport;
    }

    @Override
    public CustomerReport reportIncorrectMatchedAttr(IncorrectMatchedAttrReportRequest reportRequest) {
        log.info("Received customer report \n" + JsonUtils.pprint(reportRequest));

        //TODO: change to call matchapi
        CustomerReport customerReport = new CustomerReport();
        customerReport.setId(UUID.randomUUID().toString());

        customerReport.setType(CustomerReportType.MATCHEDATTRIBUTE);

        return customerReport;
    }

}
