package com.latticeengines.pls.service;

import com.latticeengines.domain.exposed.security.Tenant;

public interface ModelSummaryDownloadService {

    void downloadModel(Tenant tenant);
}
