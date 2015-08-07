package com.latticeengines.release.activities;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import com.latticeengines.release.error.handler.ErrorHandler;
import com.latticeengines.release.exposed.activities.BaseActivity;
import com.latticeengines.release.exposed.domain.ProcessContext;
import com.latticeengines.release.nexus.service.NexusService;

@Component("uploadProjectsToNexusActivity")
public class UploadProjectsToNexusActivity extends BaseActivity{

    @Autowired
    private NexusService nexusService;

    @Value("${release.nexus.url}")
    private String url;

    @Autowired
    public UploadProjectsToNexusActivity(@Qualifier("defaultErrorHandler") ErrorHandler errorHandler) {
        super(errorHandler);
    }

    @Override
    public ProcessContext runActivity(ProcessContext context) {
        for(String project : context.getProjectsShouldUploadToNexus()){
            ResponseEntity<String> response = nexusService.uploadArtifactToNexus(url, project, context.getReleaseVersion());
            context.setStatusCode(response.getStatusCode().value());
        }
        return context;
    }
}
