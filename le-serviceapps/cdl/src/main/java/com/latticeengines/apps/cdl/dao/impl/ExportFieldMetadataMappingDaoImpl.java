package com.latticeengines.apps.cdl.dao.impl;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.ExportFieldMetadataMappingDao;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataMapping;

@Component("exportFieldMetadataMappingDao")
public class ExportFieldMetadataMappingDaoImpl extends BaseDaoImpl<ExportFieldMetadataMapping> implements ExportFieldMetadataMappingDao {

    private Logger log = LoggerFactory.getLogger(getClass());

    @Override
    protected Class<ExportFieldMetadataMapping> getEntityClass() {
        return ExportFieldMetadataMapping.class;
    }

    @Override
    public List<ExportFieldMetadataMapping> updateExportFieldMetadataMappings(
            List<ExportFieldMetadataMapping> existingFieldMapping,
            List<ExportFieldMetadataMapping> updatedFieldMapping) {

        Map<String, ExportFieldMetadataMapping> retrievedFieldMappingMap = existingFieldMapping.stream()
                .collect(Collectors.toMap(ExportFieldMetadataMapping::getSourceField, Function.identity()));

        log.info(JsonUtils.serialize(retrievedFieldMappingMap));

        Map<String, ExportFieldMetadataMapping> updatedFieldMappingMap = updatedFieldMapping.stream()
                .collect(Collectors.toMap(ExportFieldMetadataMapping::getSourceField, Function.identity()));
        
        log.info(JsonUtils.serialize(updatedFieldMappingMap));

        LinkedList<String> unusedFieldMapping = existingFieldMapping.stream().map(ExportFieldMetadataMapping::getSourceField)
                .filter(sourceField -> !updatedFieldMappingMap.containsKey(sourceField))
                .collect(Collectors.toCollection(LinkedList::new));
        
        log.info(JsonUtils.serialize(unusedFieldMapping));

        updatedFieldMapping.forEach(fieldMapping -> {
            if (retrievedFieldMappingMap.containsKey(fieldMapping.getSourceField())) {
                ExportFieldMetadataMapping existingMapping = retrievedFieldMappingMap
                        .get(fieldMapping.getSourceField());
                log.info(JsonUtils.serialize(existingMapping));
                existingMapping.setDestinationField(fieldMapping.getDestinationField());
                existingMapping.setOverwriteValue(fieldMapping.getOverwriteValue());
                fieldMapping.setUpdated(new Date(System.currentTimeMillis()));
                super.createOrUpdate(existingMapping);
                retrievedFieldMappingMap.remove(fieldMapping.getSourceField());
            } else if (!unusedFieldMapping.isEmpty()) {
                String sourceFieldToOverwrite = unusedFieldMapping.pollFirst();
                ExportFieldMetadataMapping existingMapping = retrievedFieldMappingMap
                        .get(sourceFieldToOverwrite);
                log.info(JsonUtils.serialize(existingMapping));
                existingMapping.setSourceField(fieldMapping.getSourceField());
                existingMapping.setDestinationField(fieldMapping.getDestinationField());
                existingMapping.setOverwriteValue(fieldMapping.getOverwriteValue());
                fieldMapping.setUpdated(new Date(System.currentTimeMillis()));
                super.createOrUpdate(existingMapping);
                retrievedFieldMappingMap.remove(sourceFieldToOverwrite);
            } else {
                log.info(JsonUtils.serialize(fieldMapping));
                fieldMapping.setCreated(new Date(System.currentTimeMillis()));
                super.createOrUpdate(fieldMapping);
            }
            log.info(JsonUtils.serialize(retrievedFieldMappingMap));
        });
        
        log.info(JsonUtils.serialize(retrievedFieldMappingMap));

        if (retrievedFieldMappingMap.size() > 0) {
            retrievedFieldMappingMap.values().stream().map(ExportFieldMetadataMapping::getPid)
                    .forEach(pid -> super.deleteByPid(pid, true));
        }
        

        return updatedFieldMapping;
    }

}
