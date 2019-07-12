package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
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
        Map<String, ColumnMetadata> attributesMap = getServingMetadata(customerSpace,
                Arrays.asList(BusinessEntity.Account, BusinessEntity.Contact))
                        .collect(HashMap<String, ColumnMetadata>::new,
                                (returnMap, cm) -> returnMap.put(cm.getAttrName(), cm))
                .block();

        Map<String, ExportFieldMetadataDefaults> defaultFieldsMetadataMap = getStandardExportFields(
                channel.getLookupIdMap().getExternalSystemName()).stream()
                        .collect(Collectors.toMap(ExportFieldMetadataDefaults::getAttrName, Function.identity()));
        
        List<String> mappedFieldNames = getMappedFieldNames(channel.getLookupIdMap().getOrgId());

        List<ColumnMetadata> exportColumnMetadataList = new ArrayList<ColumnMetadata>();

        if (mappedFieldNames != null && mappedFieldNames.size() != 0) {
            mappedFieldNames.forEach(fieldName -> {
                ColumnMetadata cm = attributesMap.containsKey(fieldName) ? attributesMap.get(fieldName)
                        : constructCampaignDerivedColumnMetadata(defaultFieldsMetadataMap.get(fieldName));
                exportColumnMetadataList.add(cm);
            });
        } else {
            defaultFieldsMetadataMap.values().forEach(defaultField -> {
                ColumnMetadata cm = defaultField.getStandardField()
                        && attributesMap.containsKey(defaultField.getAttrName())
                        ? attributesMap.get(defaultField.getAttrName())
                        : constructCampaignDerivedColumnMetadata(defaultField);

                exportColumnMetadataList.add(cm);
            });

        }

        return exportColumnMetadataList;

    }

    private List<String> getMappedFieldNames(String orgId) {
        List<ExportFieldMetadataMapping> mapping = exportFieldMetadataMappingEntityMgr.findByOrgId(orgId);
        return mapping.stream().map(ExportFieldMetadataMapping::getSourceField).collect(Collectors.toList());
    }
}
