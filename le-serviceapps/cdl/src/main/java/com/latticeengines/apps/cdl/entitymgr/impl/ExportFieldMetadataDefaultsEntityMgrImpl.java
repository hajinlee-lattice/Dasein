package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.ExportFieldMetadataDefaultsDao;
import com.latticeengines.apps.cdl.entitymgr.ExportFieldMetadataDefaultsEntityMgr;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataDefaults;

@Component("exportFieldMetadataDefaultsEntityMgr")
public class ExportFieldMetadataDefaultsEntityMgrImpl implements ExportFieldMetadataDefaultsEntityMgr {

    @Inject
    ExportFieldMetadataDefaultsDao exportFieldMetadataDefaultsDao;

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public List<ExportFieldMetadataDefaults> createAll(List<ExportFieldMetadataDefaults> defaultFields) {
        exportFieldMetadataDefaultsDao.create(defaultFields);
        return defaultFields;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = true)
    public List<ExportFieldMetadataDefaults> getDefaultExportFieldMetadata(CDLExternalSystemName systemName) {
        return exportFieldMetadataDefaultsDao.getDefaultExportFields(systemName);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public List<ExportFieldMetadataDefaults> updateDefaultFields(CDLExternalSystemName systemName,
            List<ExportFieldMetadataDefaults> defaultFields) {
        defaultFields.stream().forEach(df -> {
            exportFieldMetadataDefaultsDao.update(df);
        });
        return defaultFields;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void removeBySystemName(CDLExternalSystemName systemName) {
        exportFieldMetadataDefaultsDao.deleteBySystemName(systemName);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void removeByAttrNames(CDLExternalSystemName systemName, List<String> attrNames) {
        exportFieldMetadataDefaultsDao.deleteByAttrNames(systemName, attrNames);
    }

}
