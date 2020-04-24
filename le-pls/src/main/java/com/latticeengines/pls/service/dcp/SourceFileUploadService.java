package com.latticeengines.pls.service.dcp;

import org.springframework.web.multipart.MultipartFile;

import com.latticeengines.domain.exposed.dcp.SourceFileInfo;
import com.latticeengines.domain.exposed.query.EntityType;

public interface SourceFileUploadService {

    SourceFileInfo uploadFile(String name, String displayName, boolean compressed, EntityType entityType,
                              MultipartFile file);
}
