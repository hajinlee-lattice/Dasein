package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
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
    public List<ExportFieldMetadataDefaults> getAllAttributes(CDLExternalSystemName systemName) {
        return exportFieldMetadataDefaultsEntityMgr.getAllDefaultExportFieldMetadata(systemName);
    }
    
    @Override
    public List<ExportFieldMetadataDefaults> getExportEnabledAttributes(CDLExternalSystemName systemName) {
        return exportFieldMetadataDefaultsEntityMgr.getExportEnabledDefaultFieldMetadata(systemName);
    }
    
    @Override
    public List<ExportFieldMetadataDefaults> getHistoryEnabledAttributes(CDLExternalSystemName systemName) {
        return exportFieldMetadataDefaultsEntityMgr.getHistoryEnabledDefaultFieldMetadata(systemName);
    }

    @Override
    public List<ExportFieldMetadataDefaults> updateDefaultFields(CDLExternalSystemName systemName,
            List<ExportFieldMetadataDefaults> defaultExportFields) {
        return this.updateFieldMetadataDefault(systemName, defaultExportFields, getAllAttributes(systemName));
    }

    @Override
    public void deleteBySystemName(CDLExternalSystemName systemName) {
        exportFieldMetadataDefaultsEntityMgr.removeBySystemName(systemName);
    }

    @Override
    public void deleteByAttrNames(CDLExternalSystemName systemName, List<String> attrNames) {
        exportFieldMetadataDefaultsEntityMgr.removeByAttrNames(systemName, attrNames);
    }

    private List<ExportFieldMetadataDefaults> updateFieldMetadataDefault(CDLExternalSystemName systemName, List<ExportFieldMetadataDefaults> newDefaultExportFields, List<ExportFieldMetadataDefaults> oldDefaultExportFields){
        List<ExportFieldMetadataDefaults> listToSave = new ArrayList<>();
        List<ExportFieldMetadataDefaults> listToCreate = new ArrayList<>();
        newDefaultExportFields.forEach( defaultField -> {
            ExportFieldMetadataDefaults updated = oldDefaultExportFields.stream()
                .filter( oldField -> defaultField.getAttrName().equals(oldField.getAttrName()) && defaultField.getExternalSystemName().equals((oldField.getExternalSystemName())))
                .findAny()
                .orElse(null);
            if(updated != null){
                defaultField.setPid(updated.getPid());
                listToSave.add(defaultField);
            }else {
                listToCreate.add(defaultField);
            }
        });
        List<ExportFieldMetadataDefaults> listCreated = addNewFields(systemName, listToCreate);
        List<ExportFieldMetadataDefaults> saved = exportFieldMetadataDefaultsEntityMgr.updateDefaultFields(systemName, listToSave);
        saved.addAll(listCreated);
        return saved;
    }

    private List<ExportFieldMetadataDefaults> addNewFields(CDLExternalSystemName systemName, List<ExportFieldMetadataDefaults> newFields){
        if(!newFields.isEmpty()) {
            return exportFieldMetadataDefaultsEntityMgr.createAll(newFields);
        } else {
            return newFields;
        }
    }
}
