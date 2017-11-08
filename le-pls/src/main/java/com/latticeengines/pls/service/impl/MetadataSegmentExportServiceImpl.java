package com.latticeengines.pls.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.pls.entitymanager.MetadataSegmentExportEntityMgr;
import com.latticeengines.pls.service.MetadataSegmentExportService;

@Component("metadataSegmentExportService")
public class MetadataSegmentExportServiceImpl implements MetadataSegmentExportService {

    @Autowired
    private MetadataSegmentExportEntityMgr metadataSegmentExportEntityMgr;

    @Override
    public List<MetadataSegmentExport> getSegmentExports() {
        return metadataSegmentExportEntityMgr.findAll();
    }

    @Override
    public MetadataSegmentExport getSegmentExportByExportId(String exportId) {
        return metadataSegmentExportEntityMgr.findByExportId(exportId);
    }

    @Override
    public MetadataSegmentExport createOrUpdateSegment(MetadataSegmentExport segmentExport) {
        metadataSegmentExportEntityMgr.create(segmentExport);
        return metadataSegmentExportEntityMgr.findByExportId(segmentExport.getExportId());
    }

    @Override
    public void deleteSegmentExportByExportId(String exportId) {
        metadataSegmentExportEntityMgr.deleteByExportId(exportId);
    }

}
