package com.latticeengines.release.activities;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import com.latticeengines.release.error.handler.ErrorHandler;
import com.latticeengines.release.exposed.domain.ProcessContext;

@Component("changeJenknisConfigurationActivity")
public class ChangeJenkinsConfigurationActivity extends RunJenkinsJobActivity {

    @Autowired
    public ChangeJenkinsConfigurationActivity(@Qualifier("defaultErrorHandler") ErrorHandler errorHandler) {
        super(errorHandler);
    }

    @Override
    public ProcessContext runActivity(ProcessContext context) {
        ResponseEntity<String> response = jenkinsService.updateSVNBranchName(context.getUrl(),
                String.format("release_%s", context.getReleaseVersion()));
        context.setStatusCode(response.getStatusCode().value());
        return context;
    }
}
