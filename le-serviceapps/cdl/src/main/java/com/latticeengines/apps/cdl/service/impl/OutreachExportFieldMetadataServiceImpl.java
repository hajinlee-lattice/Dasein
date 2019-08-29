package com.latticeengines.apps.cdl.service.impl;

import java.util.Arrays;
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
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataMapping;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component("outreachExportFieldMetadataService")
public class OutreachExportFieldMetadataServiceImpl extends ExportFieldMetadataServiceBase {

    private static Logger log = LoggerFactory.getLogger(OutreachExportFieldMetadataServiceImpl.class);

    @Inject
    private ExportFieldMetadataMappingEntityMgr exportFieldMetadataMappingEntityMgr;

    protected OutreachExportFieldMetadataServiceImpl() {
        super(CDLExternalSystemName.Outreach);
    }

    @Override
    public List<ColumnMetadata> getExportEnabledFields(String customerSpace, PlayLaunchChannel channel) {
        log.info("Calling OutreachExportFieldMetadataService");

        Map<String, ColumnMetadata> accountAttributesMap = getServingMetadataMap(customerSpace,
                Arrays.asList(BusinessEntity.Account));

        Map<String, ColumnMetadata> contactAttributesMap = getServingMetadataMap(customerSpace,
                Arrays.asList(BusinessEntity.Contact));

        List<String> mappedFieldNames = getMappedFieldNames(channel.getLookupIdMap().getOrgId(), channel.getLookupIdMap().getTenant().getPid());

        List<ColumnMetadata> exportColumnMetadataList;

        if (mappedFieldNames != null && mappedFieldNames.size() != 0) {
            exportColumnMetadataList = enrichExportFieldMappings(CDLExternalSystemName.Outreach, mappedFieldNames,
                    accountAttributesMap, contactAttributesMap);
        } else {
            exportColumnMetadataList = enrichDefaultFieldsMetadata(CDLExternalSystemName.Outreach,
                    accountAttributesMap, contactAttributesMap);
        }

        return exportColumnMetadataList;

    }

    private List<String> getMappedFieldNames(String orgId, Long tenantPid) {
        List<ExportFieldMetadataMapping> mapping = exportFieldMetadataMappingEntityMgr.findByOrgId(orgId, tenantPid);
        return mapping.stream().map(ExportFieldMetadataMapping::getSourceField).collect(Collectors.toList());
    }
}
