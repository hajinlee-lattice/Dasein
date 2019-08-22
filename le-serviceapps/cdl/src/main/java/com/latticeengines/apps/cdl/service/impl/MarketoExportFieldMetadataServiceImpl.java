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

        Map<String, ColumnMetadata> accountAttributesMap = getServingMetadataMap(customerSpace,
                Arrays.asList(BusinessEntity.Account));

        Map<String, ColumnMetadata> contactAttributesMap = getServingMetadataMap(customerSpace,
                Arrays.asList(BusinessEntity.Contact));

        List<String> mappedFieldNames = getMappedFieldNames(channel.getLookupIdMap().getOrgId(), channel.getLookupIdMap().getTenant().getPid());

        List<ColumnMetadata> exportColumnMetadataList;

        if (mappedFieldNames != null && mappedFieldNames.size() != 0) {
            exportColumnMetadataList = enrichExportFieldMappings(CDLExternalSystemName.Marketo, mappedFieldNames,
                    accountAttributesMap, contactAttributesMap);
        } else {
            exportColumnMetadataList = enrichDefaultFieldsMetadata(CDLExternalSystemName.Marketo,
                    accountAttributesMap, contactAttributesMap);
        }

        return exportColumnMetadataList;

    }

    private List<String> getMappedFieldNames(String orgId, Long tenantPid) {
        List<ExportFieldMetadataMapping> mapping = exportFieldMetadataMappingEntityMgr.findByOrgId(orgId, tenantPid);
        return mapping.stream().map(ExportFieldMetadataMapping::getSourceField).collect(Collectors.toList());
    }
}
