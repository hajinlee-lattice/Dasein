package com.latticeengines.proxy.exposed.pls;

import java.util.List;

import org.springframework.lang.NonNull;

import com.latticeengines.domain.exposed.cdl.S3ImportEmailInfo;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfigContext;
import com.latticeengines.domain.exposed.workflow.Job;

public interface PlsInternalProxy {

    ScoringRequestConfigContext retrieveScoringRequestConfigContext(String configUuid);

    void sendS3TemplateUpdateEmail(String tenantId, S3ImportEmailInfo emailInfo);

    void sendS3TemplateCreateEmail(String tenantId, S3ImportEmailInfo emailInfo);

    void sendS3ImportEmail(String result, String tenantId, S3ImportEmailInfo emailInfo);

    List<Job> findJobsBasedOnActionIdsAndType(@NonNull String customerSpace, List<Long> actionPids,
                                              ActionType actionType);
}
