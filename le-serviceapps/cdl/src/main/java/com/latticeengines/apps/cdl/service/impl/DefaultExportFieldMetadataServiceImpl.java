package com.latticeengines.apps.cdl.service.impl;

import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.cdl.channel.AudienceType;

@Component("defaultExportFieldMetadataService")
public class DefaultExportFieldMetadataServiceImpl extends ExportFieldMetadataServiceBase {

    DefaultExportFieldMetadataServiceImpl() {
        super(Arrays.asList(CDLExternalSystemName.LinkedIn, //
                CDLExternalSystemName.Facebook, //
                CDLExternalSystemName.GoogleAds, //
                CDLExternalSystemName.Eloqua, //
                CDLExternalSystemName.Salesforce));
    }

    @Override
    public List<ColumnMetadata> getExportEnabledFields(String customerSpace, PlayLaunchChannel channel) {
        CDLExternalSystemName externalSystemName = channel.getLookupIdMap().getExternalSystemName();

        if (externalSystemName.equals(CDLExternalSystemName.LinkedIn)) {
            AudienceType audienceType = channel.getChannelConfig().getAudienceType();

            return enrichDefaultFieldsMetadata(customerSpace, externalSystemName, audienceType);
        }

        return enrichDefaultFieldsMetadata(customerSpace, externalSystemName);
    }

}
