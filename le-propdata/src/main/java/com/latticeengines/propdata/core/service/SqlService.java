package com.latticeengines.propdata.core.service;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.propdata.ExportRequest;
import com.latticeengines.domain.exposed.propdata.ImportRequest;

public interface SqlService {
    ApplicationId importTable(ImportRequest importRequest, Boolean Sync);
    ApplicationId exportTable(ExportRequest exportRequest, Boolean sync);
}
