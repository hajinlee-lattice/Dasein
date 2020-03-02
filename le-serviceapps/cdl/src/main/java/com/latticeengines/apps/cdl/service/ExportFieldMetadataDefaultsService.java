package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataDefaults;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public interface ExportFieldMetadataDefaultsService {

    List<ExportFieldMetadataDefaults> createDefaultExportFields(List<ExportFieldMetadataDefaults> defaultExportFields);

    List<ExportFieldMetadataDefaults> getAllAttributes(CDLExternalSystemName systemName);

    List<ExportFieldMetadataDefaults> getExportEnabledAttributes(CDLExternalSystemName systemName);

    List<ExportFieldMetadataDefaults> getExportEnabledAttributesForEntity(CDLExternalSystemName systemName,
            BusinessEntity entity);

    List<ExportFieldMetadataDefaults> getHistoryEnabledAttributes(CDLExternalSystemName systemName);

    List<ExportFieldMetadataDefaults> updateDefaultFields(CDLExternalSystemName systemName,
            List<ExportFieldMetadataDefaults> defaultExportFields);

    void deleteBySystemName(CDLExternalSystemName systemName);

    void deleteByAttrNames(CDLExternalSystemName systemName, List<String> attrNames);
}
