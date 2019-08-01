package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataDefaults;

public interface ExportFieldMetadataDefaultsEntityMgr {

    List<ExportFieldMetadataDefaults> createAll(List<ExportFieldMetadataDefaults> defaultFields);

    List<ExportFieldMetadataDefaults> getAllDefaultExportFieldMetadata(CDLExternalSystemName systemName);

    List<ExportFieldMetadataDefaults> getExportEnabledDefaultFieldMetadata(CDLExternalSystemName systemName);

    List<ExportFieldMetadataDefaults> getHistoryEnabledDefaultFieldMetadata(CDLExternalSystemName systemName);

    List<ExportFieldMetadataDefaults> updateDefaultFields(CDLExternalSystemName systemName,
            List<ExportFieldMetadataDefaults> defaultFields);

    void removeBySystemName(CDLExternalSystemName systemName);

    void removeByAttrNames(CDLExternalSystemName systemName, List<String> attrNames);

}
