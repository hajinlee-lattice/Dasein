package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataDefaults;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.cdl.channel.S3ChannelConfig;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component("s3ExportFieldMetadataService")
public class S3ExportFieldMetadataServiceImpl extends ExportFieldMetadataServiceBase {

    private static Logger log = LoggerFactory.getLogger(S3ExportFieldMetadataServiceImpl.class);

    protected S3ExportFieldMetadataServiceImpl() {
        super(CDLExternalSystemName.AWS_S3);
    }

    @Override
    public List<ColumnMetadata> getExportEnabledFields(String customerSpace, PlayLaunchChannel channel) {
        log.info("Calling S3ExportFieldMetadataService");
        Map<String, ColumnMetadata> attributesMap = getServingMetadataForEntity(customerSpace, BusinessEntity.Account)
                .collect(HashMap<String, ColumnMetadata>::new, (returnMap, cm) -> returnMap.put(cm.getAttrName(), cm))
                .block();

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

        S3ChannelConfig channelConfig = (S3ChannelConfig) channel.getChannelConfig();

        if (channelConfig.isIncludeExportAttributes()) {
            List<ColumnMetadata> exportEnabledAttributes = attributesMap.values().stream()
                    .filter(cm -> !defaultExportFieldsMap.containsKey(cm.getAttrName()))
                    .collect(Collectors.toList());
            result.addAll(exportEnabledAttributes);
        }

        return result;
    }
}
