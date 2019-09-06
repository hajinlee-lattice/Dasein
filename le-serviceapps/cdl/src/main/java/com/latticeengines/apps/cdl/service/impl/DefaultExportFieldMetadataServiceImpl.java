package com.latticeengines.apps.cdl.service.impl;

import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;

@Component("defaultExportFieldMetadataService")
public class DefaultExportFieldMetadataServiceImpl extends ExportFieldMetadataServiceBase {

    DefaultExportFieldMetadataServiceImpl() {
        super(Arrays.asList(CDLExternalSystemName.LinkedIn, CDLExternalSystemName.Facebook,
                CDLExternalSystemName.GoogleAds, CDLExternalSystemName.Eloqua, CDLExternalSystemName.Salesforce));
    }

    @Override
    public List<ColumnMetadata> getExportEnabledFields(String customerSpace, PlayLaunchChannel channel) {
        return enrichDefaultFieldsMetadata(customerSpace, channel.getLookupIdMap().getExternalSystemName());
    }

}
