package com.latticeengines.apps.cdl.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataDefaults;

public interface ExportFieldMetadataDefaultsDao extends BaseDao<ExportFieldMetadataDefaults> {

    List<ExportFieldMetadataDefaults> getDefaultExportFields(CDLExternalSystemName systemName);

    void deleteBySystemName(CDLExternalSystemName systemName);

    void deleteByAttrNames(CDLExternalSystemName systemName, List<String> attrNames);

}
