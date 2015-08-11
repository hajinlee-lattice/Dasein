package com.latticeengines.release.hipchat.activities;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import com.latticeengines.release.error.handler.ErrorHandler;
import com.latticeengines.release.exposed.activities.BaseActivity;
import com.latticeengines.release.exposed.domain.ProcessContext;
import com.latticeengines.release.hipchat.service.HipChatService;

@Component("startReleaseNotificationActivity")
public class StartReleaseNotificationActivity extends BaseActivity {

    @Autowired
    private HipChatService hipchatService;

    @Value("${release.hipchat.url}")
    private String url;

    @Autowired
    public StartReleaseNotificationActivity(@Qualifier("hipchatServiceErrorHandler") ErrorHandler errorHandler) {
        super(errorHandler);
    }

    @Override
    public ProcessContext runActivity(ProcessContext context) {
        ResponseEntity<String> response = hipchatService.sendNotification(url, "green", "Release Process Started!");
        context.setStatusCode(response.getStatusCode().value());
        return context;
    }

}
