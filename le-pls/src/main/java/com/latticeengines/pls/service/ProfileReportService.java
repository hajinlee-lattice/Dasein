package com.latticeengines.pls.service;

import com.latticeengines.domain.exposed.cdl.AtlasProfileReportStatus;

public interface ProfileReportService {

    AtlasProfileReportStatus refreshProfile();

    AtlasProfileReportStatus getStatus();

}
