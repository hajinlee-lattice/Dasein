package com.latticeengines.pls.service;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.latticeengines.domain.exposed.workflow.Job;

public interface OrphanRecordsService {

    String getOrphanRecordsCount(String orphanType);

    Job submitOrphanRecordsWorkflow(String orphanType, HttpServletResponse response);

    void downloadOrphanArtifact(String exportId, HttpServletRequest request, HttpServletResponse response);
}
