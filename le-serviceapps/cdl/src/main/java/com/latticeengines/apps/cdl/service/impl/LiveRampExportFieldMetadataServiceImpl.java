package com.latticeengines.apps.cdl.service.impl;

import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.cdl.channel.AudienceType;

@Component("liveRampExportFieldMetadataService")
public class LiveRampExportFieldMetadataServiceImpl extends ExportFieldMetadataServiceBase {

    LiveRampExportFieldMetadataServiceImpl() {
        super(Arrays.asList(
                CDLExternalSystemName.Adobe_Audience_Mgr, //
                CDLExternalSystemName.AppNexus, //
                CDLExternalSystemName.Google_Display_N_Video_360, //
                CDLExternalSystemName.MediaMath, //
                CDLExternalSystemName.TradeDesk, //
                CDLExternalSystemName.Verizon_Media));
    }

    @Override
    public List<ColumnMetadata> getExportEnabledFields(String customerSpace, PlayLaunchChannel channel) {
        // Placeholder for when the actual attribute is confirmed
        CDLExternalSystemName externalSystemName = channel.getLookupIdMap().getExternalSystemName();
        AudienceType audienceType = channel.getChannelConfig().getAudienceType();

        return enrichDefaultFieldsMetadata(customerSpace, externalSystemName, audienceType);
    }
}
