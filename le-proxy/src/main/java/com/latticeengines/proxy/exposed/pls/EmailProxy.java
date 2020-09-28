package com.latticeengines.proxy.exposed.pls;

import com.latticeengines.domain.exposed.cdl.AtlasExport;
import com.latticeengines.domain.exposed.cdl.OrphanRecordsExportRequest;
import com.latticeengines.domain.exposed.cdl.S3ImportEmailInfo;
import com.latticeengines.domain.exposed.dcp.UploadEmailInfo;
import com.latticeengines.domain.exposed.pls.AdditionalEmailInfo;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;

public interface EmailProxy {

    boolean sendS3TemplateUpdateEmail(String tenantId, S3ImportEmailInfo emailInfo);

    boolean sendS3TemplateCreateEmail(String tenantId, S3ImportEmailInfo emailInfo);

    boolean sendS3ImportEmail(String result, String tenantId, S3ImportEmailInfo emailInfo);

    void sendMetadataSegmentExportEmail(String result, String tenantId, MetadataSegmentExport export);

    void sendAtlasExportEmail(String result, String tenantId, AtlasExport export);

    void sendOrphanRecordsExportEmail(String result, String tenantId, OrphanRecordsExportRequest export);

    void sendPlsEnrichInternalAttributeEmail(String result, String tenantId,
                                             AdditionalEmailInfo info);

    boolean sendCDLProcessAnalyzeEmail(String result, String tenantId, AdditionalEmailInfo info);

    void sendPlsScoreEmail(String result, String tenantId, AdditionalEmailInfo info);

    void sendPlsCreateModelEmail(String result, String tenantId, AdditionalEmailInfo info);

    void sendUploadEmail(UploadEmailInfo uploadEmailInfo);

    void sendPlayLaunchErrorEmail(String tenantId, String user, PlayLaunch playLaunch);

    boolean sendPlayLaunchChannelExpiringEmail(String tenantId, PlayLaunchChannel playLaunchChannel);
}
