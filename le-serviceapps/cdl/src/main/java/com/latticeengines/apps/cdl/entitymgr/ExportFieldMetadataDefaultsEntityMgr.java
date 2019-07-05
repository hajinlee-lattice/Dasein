package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataDefaults;

public interface ExportFieldMetadataDefaultsEntityMgr {

    List<ExportFieldMetadataDefaults> createAll(List<ExportFieldMetadataDefaults> defaultFields);

    List<ExportFieldMetadataDefaults> getDefaultExportFieldMetadata(CDLExternalSystemName systemName);

    void deleteByPid(Long pid);

}
