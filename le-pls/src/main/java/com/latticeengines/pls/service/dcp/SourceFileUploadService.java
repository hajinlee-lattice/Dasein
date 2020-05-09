package com.latticeengines.pls.service.dcp;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.web.multipart.MultipartFile;

import com.latticeengines.domain.exposed.dcp.DCPImportRequest;
import com.latticeengines.domain.exposed.dcp.SourceFileInfo;
import com.latticeengines.domain.exposed.query.EntityType;

public interface SourceFileUploadService {

    SourceFileInfo uploadFile(String name, String displayName, boolean compressed, EntityType entityType,
                              MultipartFile file);

    ApplicationId submitSourceImport(DCPImportRequest importRequest);
}
