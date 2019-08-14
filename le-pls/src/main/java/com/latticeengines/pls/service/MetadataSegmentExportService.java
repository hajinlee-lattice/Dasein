package com.latticeengines.pls.service;

import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;

public interface MetadataSegmentExportService {
    List<MetadataSegmentExport> getSegmentExports();

    MetadataSegmentExport getSegmentExportByExportId(String exportId);

    MetadataSegmentExport createSegmentExportJob(MetadataSegmentExport segmentExport);

    MetadataSegmentExport updateSegmentExportJob(MetadataSegmentExport segmentExport);

    void deleteSegmentExportByExportId(String exportId);

    void downloadSegmentExportResult(String exportId, HttpServletRequest request, HttpServletResponse response);

    String getExportedFilePath(MetadataSegmentExport metadataSegmentExport);

    MetadataSegmentExport createOrphanRecordThruMgr(MetadataSegmentExport metadataSegmentExport);
}
