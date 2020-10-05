package com.latticeengines.proxy.exposed.cdl;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.cdl.AtlasProfileReportRequest;
import com.latticeengines.domain.exposed.cdl.AtlasProfileReportStatus;

public interface ProfileReportProxy {

    ApplicationId generateProfileReport(String customerSpace, AtlasProfileReportRequest request);

    AtlasProfileReportStatus getProfileReportStatus(String customerSpace);

}
