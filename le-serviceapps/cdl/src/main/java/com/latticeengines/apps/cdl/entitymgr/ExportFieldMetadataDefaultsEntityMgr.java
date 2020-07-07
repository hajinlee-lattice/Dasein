package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataDefaults;
import com.latticeengines.domain.exposed.pls.cdl.channel.AudienceType;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public interface ExportFieldMetadataDefaultsEntityMgr {

    List<ExportFieldMetadataDefaults> createAll(List<ExportFieldMetadataDefaults> defaultFields);

    List<ExportFieldMetadataDefaults> getAllDefaultExportFieldMetadata(CDLExternalSystemName systemName);

    List<ExportFieldMetadataDefaults> getExportEnabledDefaultFieldMetadata(CDLExternalSystemName systemName);

    List<ExportFieldMetadataDefaults> getExportEnabledDefaultFieldMetadataForEntity(CDLExternalSystemName systemName,
            BusinessEntity entity);

    List<ExportFieldMetadataDefaults> getExportEnabledDefaultFieldMetadataForAudienceType(
            CDLExternalSystemName systemName, AudienceType audienceType);

    List<ExportFieldMetadataDefaults> getHistoryEnabledDefaultFieldMetadata(CDLExternalSystemName systemName);

    List<ExportFieldMetadataDefaults> updateDefaultFields(CDLExternalSystemName systemName,
            List<ExportFieldMetadataDefaults> defaultFields);

    void removeBySystemName(CDLExternalSystemName systemName);

    void removeByAttrNames(CDLExternalSystemName systemName, List<String> attrNames);

}
