package com.latticeengines.pls.service;

import java.util.List;

import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;

public interface MetadataSegmentExportService {
    List<MetadataSegmentExport> getSegmentExports();

    MetadataSegmentExport getSegmentExportByExportId(String exportId);

    MetadataSegmentExport createOrUpdateSegment(MetadataSegmentExport segmentExport);

    void deleteSegmentExportByExportId(String name);
}
