package com.latticeengines.apps.cdl.service.impl;

import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;

@Component("outreachExportFieldMetadataService")
public class OutreachExportFieldMetadataServiceImpl extends ExportFieldMetadataServiceBase {

    private static Logger log = LoggerFactory.getLogger(OutreachExportFieldMetadataServiceImpl.class);


    protected OutreachExportFieldMetadataServiceImpl() {
        super(CDLExternalSystemName.Outreach);
    }

    @Override
    public List<ColumnMetadata> getExportEnabledFields(String customerSpace, PlayLaunchChannel channel) {
        log.info("Calling OutreachExportFieldMetadataService");

        List<ColumnMetadata> enrichedDefaultFields = enrichDefaultFieldsMetadata(customerSpace, channel.getLookupIdMap().getExternalSystemName());

        return enrichedDefaultFields.stream().filter(cm-> cm.getAttrName().equals("SDR_Email") && cm.isCampaignDerivedField()).collect(Collectors.toList());

    }
}
