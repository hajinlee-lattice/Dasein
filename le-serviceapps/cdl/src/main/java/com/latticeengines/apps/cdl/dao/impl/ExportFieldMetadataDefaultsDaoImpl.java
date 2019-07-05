package com.latticeengines.apps.cdl.dao.impl;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.ExportFieldMetadataDefaultsDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataDefaults;

@Component("exportFieldMetadataDefaultsDao")
public class ExportFieldMetadataDefaultsDaoImpl extends BaseDaoImpl<ExportFieldMetadataDefaults>
        implements ExportFieldMetadataDefaultsDao {

    @Override
    public List<ExportFieldMetadataDefaults> getDefaultExportFields(CDLExternalSystemName systemName) {
        return this.findAllByFields("externalSystemName", systemName, "exportEnabled", true);
    }

    @Override
    protected Class<ExportFieldMetadataDefaults> getEntityClass() {
        return ExportFieldMetadataDefaults.class;
    }

}

