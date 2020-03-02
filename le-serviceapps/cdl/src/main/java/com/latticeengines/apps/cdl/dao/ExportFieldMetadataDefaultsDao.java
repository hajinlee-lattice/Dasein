package com.latticeengines.apps.cdl.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataDefaults;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public interface ExportFieldMetadataDefaultsDao extends BaseDao<ExportFieldMetadataDefaults> {

    List<ExportFieldMetadataDefaults> getAllDefaultExportFields(CDLExternalSystemName systemName);

    List<ExportFieldMetadataDefaults> getHistoryEnabledDefaultFields(CDLExternalSystemName systemName);

    List<ExportFieldMetadataDefaults> getExportEnabledDefaultFields(CDLExternalSystemName systemName);

    List<ExportFieldMetadataDefaults> getExportEnabledDefaultFieldsForEntity(CDLExternalSystemName systemName,
            BusinessEntity entity);

    void deleteBySystemName(CDLExternalSystemName systemName);

    void deleteByAttrNames(CDLExternalSystemName systemName, List<String> attrNames);

}
