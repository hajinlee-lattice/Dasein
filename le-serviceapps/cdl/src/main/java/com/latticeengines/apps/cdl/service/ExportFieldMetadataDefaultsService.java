package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataDefaults;

public interface ExportFieldMetadataDefaultsService {

    List<ExportFieldMetadataDefaults> createDefaultExportFields(List<ExportFieldMetadataDefaults> defaultExportFields);

    List<ExportFieldMetadataDefaults> getAttributes(CDLExternalSystemName systemName);

    void delete(List<ExportFieldMetadataDefaults> defaultExportFields);

}
