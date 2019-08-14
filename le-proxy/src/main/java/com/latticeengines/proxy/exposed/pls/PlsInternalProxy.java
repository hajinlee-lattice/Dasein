package com.latticeengines.proxy.exposed.pls;

import java.util.List;

import org.springframework.lang.NonNull;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.AtlasExport;
import com.latticeengines.domain.exposed.cdl.S3ImportEmailInfo;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfigContext;
import com.latticeengines.domain.exposed.workflow.Job;

public interface PlsInternalProxy {

    ScoringRequestConfigContext retrieveScoringRequestConfigContext(String configUuid);

    void sendS3TemplateUpdateEmail(String tenantId, S3ImportEmailInfo emailInfo);

    void sendS3TemplateCreateEmail(String tenantId, S3ImportEmailInfo emailInfo);

    void sendS3ImportEmail(String result, String tenantId, S3ImportEmailInfo emailInfo);

    List<Job> findJobsBasedOnActionIdsAndType(@NonNull String customerSpace, List<Long> actionPids,
                                              ActionType actionType);

    void sendMetadataSegmentExportEmail(String result, String tenantId, MetadataSegmentExport export);

    MetadataSegmentExport getMetadataSegmentExport(CustomerSpace customerSpace, String exportId);

    void sendAtlasExportEmail(String result, String tenantId, AtlasExport export);

    MetadataSegmentExport updateMetadataSegmentExport(CustomerSpace customerSpace, //
                                                      String exportId, MetadataSegmentExport.Status state);
}
