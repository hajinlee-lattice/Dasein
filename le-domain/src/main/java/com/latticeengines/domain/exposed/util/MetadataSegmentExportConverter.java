package com.latticeengines.domain.exposed.util;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.cdl.AtlasExport;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;

public class MetadataSegmentExportConverter {

    public static AtlasExport convertToAtlasExport(MetadataSegmentExport metadataSegmentExport) {
        if (metadataSegmentExport == null) {
            return null;
        }
        AtlasExport atlasExport = new AtlasExport();
        atlasExport.setUuid(metadataSegmentExport.getExportId());
        if (metadataSegmentExport.getSegment() != null) {
            atlasExport.setSegmentName(metadataSegmentExport.getSegment().getName());
        }
        atlasExport.setTenant(metadataSegmentExport.getTenant());
        atlasExport.setCreatedBy(metadataSegmentExport.getCreatedBy());
        atlasExport.setAccountFrontEndRestriction(metadataSegmentExport.getAccountFrontEndRestriction());
        atlasExport.setContactFrontEndRestriction(metadataSegmentExport.getContactFrontEndRestriction());
        atlasExport.setApplicationId(metadataSegmentExport.getApplicationId());
        atlasExport.setExportType(metadataSegmentExport.getType());
        atlasExport.setStatus(metadataSegmentExport.getStatus());
        atlasExport.setTenantId(metadataSegmentExport.getTenantId());
        atlasExport.setCleanupBy(metadataSegmentExport.getCleanupBy());
        atlasExport.setSegmentName(metadataSegmentExport.getExportPrefix());
        atlasExport.setCreated(metadataSegmentExport.getCreated());
        atlasExport.setUpdated(metadataSegmentExport.getUpdated());
        return atlasExport;
    }

    public static MetadataSegmentExport convertToMetadataSegmentExport(AtlasExport atlasExport) {
        if (atlasExport == null) {
            return null;
        }
        MetadataSegmentExport metadataSegmentExport = new MetadataSegmentExport();
        metadataSegmentExport.setExportPrefix(atlasExport.getSegmentName());
        metadataSegmentExport.setExportId(atlasExport.getUuid());
        metadataSegmentExport.setTenant(atlasExport.getTenant());
        metadataSegmentExport.setTenantId(atlasExport.getTenantId());
        metadataSegmentExport.setType(atlasExport.getExportType());
        metadataSegmentExport.setApplicationId(atlasExport.getApplicationId());
        metadataSegmentExport.setAccountFrontEndRestriction(atlasExport.getAccountFrontEndRestriction());
        metadataSegmentExport.setContactFrontEndRestriction(atlasExport.getContactFrontEndRestriction());
        metadataSegmentExport.setCreatedBy(atlasExport.getCreatedBy());
        metadataSegmentExport.setStatus(atlasExport.getStatus());
        if (CollectionUtils.isNotEmpty(atlasExport.getFilesUnderSystemPath())) {
            metadataSegmentExport.setPath(atlasExport.getFilesUnderSystemPath().get(0));
        } else if (CollectionUtils.isNotEmpty(atlasExport.getFilesUnderDropFolder())) {
            metadataSegmentExport.setPath(atlasExport.getFilesUnderDropFolder().get(0));
        }
        metadataSegmentExport.setCleanupBy(atlasExport.getCleanupBy());
        metadataSegmentExport.setExportPrefix(atlasExport.getSegmentName());
        metadataSegmentExport.setCreated(atlasExport.getCreated());
        metadataSegmentExport.setUpdated(atlasExport.getUpdated());
        return metadataSegmentExport;
    }

}
