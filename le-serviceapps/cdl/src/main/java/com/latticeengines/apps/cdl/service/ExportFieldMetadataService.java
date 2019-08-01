package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;

public interface ExportFieldMetadataService {

    List<ColumnMetadata> getExportEnabledFields(String customerSpace, PlayLaunchChannel channel);
}
