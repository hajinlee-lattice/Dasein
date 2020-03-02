package com.latticeengines.apps.cdl.service.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.cdl.channel.AudienceType;
import com.latticeengines.domain.exposed.pls.cdl.channel.S3ChannelConfig;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component("s3ExportFieldMetadataService")
public class S3ExportFieldMetadataServiceImpl extends ExportFieldMetadataServiceBase {

    private static final Logger log = LoggerFactory.getLogger(S3ExportFieldMetadataServiceImpl.class);

    protected S3ExportFieldMetadataServiceImpl() {
        super(CDLExternalSystemName.AWS_S3);
    }

    /*
     * PLS-16440 For Account based S3 launch, only account attributes will be in
     * final csv. For Contact based S3 launch, both account and contact
     * attributes are present.
     */

    @Override
    public List<ColumnMetadata> getExportEnabledFields(String customerSpace, PlayLaunchChannel channel) {
        log.info("Calling S3ExportFieldMetadataServicem for channle " + channel.getId());
        S3ChannelConfig channelConfig = (S3ChannelConfig) channel.getChannelConfig();
        AudienceType audienceType = channelConfig.getAudienceType();
        BusinessEntity entity = BusinessEntity.Contact;
        if (audienceType != null) {
            entity = audienceType.asBusinessEntity();
        } else {
            log.warn("AudienceType for " + channel.getId() + " is null");
        }
        boolean exportBothAccountAndContactAttr = BusinessEntity.Contact.equals(entity);

        Map<String, ColumnMetadata> accountAttributesMap = getServingMetadataMap(customerSpace,
                Arrays.asList(BusinessEntity.Account));

        Map<String, ColumnMetadata> contactAttributesMap = exportBothAccountAndContactAttr
                ? getServingMetadataMap(customerSpace, Arrays.asList(BusinessEntity.Contact))
                : Collections.emptyMap();

        List<ColumnMetadata> exportColumnMetadataList = exportBothAccountAndContactAttr
                ? enrichDefaultFieldsMetadata(CDLExternalSystemName.AWS_S3, accountAttributesMap, contactAttributesMap)
                : enrichDefaultFieldsMetadata(CDLExternalSystemName.AWS_S3, accountAttributesMap, contactAttributesMap,
                        entity);

        if (channelConfig.isIncludeExportAttributes()) {
            exportColumnMetadataList.addAll(accountAttributesMap.values());
            if (exportBothAccountAndContactAttr) {
                exportColumnMetadataList.addAll(contactAttributesMap.values());
            }
            exportColumnMetadataList.addAll(getServingMetadata(customerSpace,
                    Arrays.asList(BusinessEntity.Rating, BusinessEntity.PurchaseHistory, BusinessEntity.CuratedAccount))
                            .collect(Collectors.toList()).block());
        }

        return exportColumnMetadataList;
    }
}
