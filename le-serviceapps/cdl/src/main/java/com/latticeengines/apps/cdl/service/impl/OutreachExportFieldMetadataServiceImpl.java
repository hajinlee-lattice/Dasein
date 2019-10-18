package com.latticeengines.apps.cdl.service.impl;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.query.BusinessEntity;

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
        // Get the account owner for the lookupid
        String accountOwner = channel.getLookupIdMap().getProspectOwner();
        log.info("Outreach account owner " + accountOwner);
        if(accountOwner == null) {
            throw new LedpException(LedpCode.LEDP_18233, new String[]{"Account Owner"});
        }
        Map<String, ColumnMetadata> accountAttributesMap = getServingMetadataMap(customerSpace, Arrays.asList(BusinessEntity.Account));
        if(accountAttributesMap.get(accountOwner) == null){
            throw new LedpException(LedpCode.LEDP_18233, new String[] { "Account Owner..." });
        }
        //From the enreached fields retrieves the account owner and changes the displayName
        ColumnMetadata accountOwnerColumnMetadata = accountAttributesMap.get(accountOwner);
        accountOwnerColumnMetadata.setDisplayName("SDR_Email");
        enrichedDefaultFields.add(accountOwnerColumnMetadata);
        return enrichedDefaultFields;
    }
}
