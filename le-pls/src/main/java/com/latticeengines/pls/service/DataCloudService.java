package com.latticeengines.pls.service;

import com.latticeengines.domain.exposed.datacloud.customer.CustomerReport;
import com.latticeengines.domain.exposed.pls.IncorrectLookupReportRequest;
import com.latticeengines.domain.exposed.pls.IncorrectMatchedAttrReportRequest;

public interface DataCloudService {

    CustomerReport reportIncorrectLookup(IncorrectLookupReportRequest reportRequest);

    CustomerReport reportIncorrectMatchedAttr(IncorrectMatchedAttrReportRequest reportRequest);

}
