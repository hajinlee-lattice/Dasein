package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.ExportFieldMetadataMappingEntityMgr;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataDefaults;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataMapping;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component("marketoExportFieldMetadataService")
public class MarketoExportFieldMetadataServiceImpl extends ExportFieldMetadataServiceBase {

    private static Logger log = LoggerFactory.getLogger(MarketoExportFieldMetadataServiceImpl.class);

    @Inject
    private ExportFieldMetadataMappingEntityMgr exportFieldMetadataMappingEntityMgr;

    protected MarketoExportFieldMetadataServiceImpl() {
        super(CDLExternalSystemName.Marketo);
    }

    @Override
    public List<ColumnMetadata> getExportEnabledFields(String customerSpace, PlayLaunchChannel channel) {
        log.info("Calling MarketoExportFieldMetadataService");
        Map<String, ColumnMetadata> attributesMap = getServingMetadataForEntity(customerSpace, BusinessEntity.Account)
                .collect(HashMap<String, ColumnMetadata>::new, (returnMap, cm) -> returnMap.put(cm.getAttrName(), cm))
                .block();

        getServingMetadataForEntity(customerSpace, BusinessEntity.Contact)
                .collect(Object::new, (__, cm) -> attributesMap.put(cm.getAttrName(), cm)).block();

        List<ExportFieldMetadataDefaults> defaultFieldsMetadata = getStandardExportFields(
                channel.getLookupIdMap().getExternalSystemName());

        List<ColumnMetadata> result = new ArrayList<ColumnMetadata>();
        Map<String, ColumnMetadata> defaultExportFieldsMap = new HashMap<String, ColumnMetadata>();

        defaultFieldsMetadata.forEach(field -> {
            ColumnMetadata cm = field.getStandardField() && attributesMap.containsKey(field.getAttrName())
                    ? attributesMap.get(field.getAttrName())
                    : constructNonStandardColumnMetadata(field);

            result.add(cm);
            defaultExportFieldsMap.put(field.getAttrName(), cm);
        });

        List<String> customizedAttributes = getCustomizedFields(channel.getLookupIdMap().getOrgId());

        List<ColumnMetadata> customAttributes = customizedAttributes.stream()
                .filter(attr -> !defaultExportFieldsMap.containsKey(attr)).filter(attributesMap::containsKey)
                .map(attr -> attributesMap.get(attr)).collect(Collectors.toList());

        result.addAll(customAttributes);

        return result;

    }

    private List<String> getCustomizedFields(String orgId) {
        List<ExportFieldMetadataMapping> mapping = exportFieldMetadataMappingEntityMgr.findByOrgId(orgId);

        return mapping.stream().map(ExportFieldMetadataMapping::getSourceField).collect(Collectors.toList());
    }

}
