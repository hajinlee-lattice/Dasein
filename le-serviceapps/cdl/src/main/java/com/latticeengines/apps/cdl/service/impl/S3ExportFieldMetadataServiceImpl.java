package com.latticeengines.apps.cdl.service.impl;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
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
        
        Map<String, ColumnMetadata> accountAttributesMap = getServingMetadataMap(customerSpace,
                Arrays.asList(BusinessEntity.Account));
        
        Map<String, ColumnMetadata> contactAttributesMap = getServingMetadataMap(customerSpace,
                Arrays.asList(BusinessEntity.Contact));

        List<ColumnMetadata> exportColumnMetadataList = enrichDefaultFieldsMetadata(CDLExternalSystemName.AWS_S3,
                accountAttributesMap, contactAttributesMap);

        S3ChannelConfig channelConfig = (S3ChannelConfig) channel.getChannelConfig();

        if (channelConfig.isIncludeExportAttributes()) {
            exportColumnMetadataList.addAll(accountAttributesMap.values());
            exportColumnMetadataList.addAll(contactAttributesMap.values());
            exportColumnMetadataList.addAll(getServingMetadata(customerSpace,
                    Arrays.asList(BusinessEntity.Rating, BusinessEntity.PurchaseHistory, BusinessEntity.CuratedAccount))
                            .collect(Collectors.toList()).block());
        }

        return exportColumnMetadataList;
    }
}
