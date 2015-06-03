package com.latticeengines.remote.exposed.service;

import java.util.List;

import com.latticeengines.domain.exposed.dataloader.InstallResult;
import com.latticeengines.domain.exposed.dataloader.InstallTemplateRequest;
import com.latticeengines.domain.exposed.pls.Segment;

public interface DataLoaderService {

    List<Segment> getSegments(String tenantName, String dlUrl);

    InstallResult setSegments(String tenantName, String dlUrl, List<Segment> segments);

    InstallResult installVisiDBStructureFile(InstallTemplateRequest request, String dlUrl);
}
