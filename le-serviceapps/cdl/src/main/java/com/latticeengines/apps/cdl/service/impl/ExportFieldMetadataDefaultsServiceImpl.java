package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.ExportFieldMetadataDefaultsEntityMgr;
import com.latticeengines.apps.cdl.service.ExportFieldMetadataDefaultsService;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataDefaults;

@Component("exportFieldMetadataDefaultsService")
public class ExportFieldMetadataDefaultsServiceImpl implements ExportFieldMetadataDefaultsService {

    private static Logger log = LoggerFactory.getLogger(ExportFieldMetadataDefaultsServiceImpl.class);

    @Inject
    ExportFieldMetadataDefaultsEntityMgr exportFieldMetadataDefaultsEntityMgr;

    @Override
    public List<ExportFieldMetadataDefaults> createDefaultExportFields(
            List<ExportFieldMetadataDefaults> defaultExportFields) {
        return exportFieldMetadataDefaultsEntityMgr.createAll(defaultExportFields);
    }

    @Override
    public List<ExportFieldMetadataDefaults> getAttributes(CDLExternalSystemName systemName) {
        return exportFieldMetadataDefaultsEntityMgr.getDefaultExportFieldMetadata(systemName);
    }

    @Override
    public List<ExportFieldMetadataDefaults> updateDefaultFields(CDLExternalSystemName systemName,
            List<ExportFieldMetadataDefaults> defaultExportFields) {
        return exportFieldMetadataDefaultsEntityMgr.updateDefaultFields(systemName, defaultExportFields);
    }

    @Override
    public void deleteBySystemName(CDLExternalSystemName systemName) {
        exportFieldMetadataDefaultsEntityMgr.removeBySystemName(systemName);
    }

    @Override
    public void deleteByAttrNames(CDLExternalSystemName systemName, List<String> attrNames) {
        exportFieldMetadataDefaultsEntityMgr.removeByAttrNames(systemName, attrNames);
    }
}
