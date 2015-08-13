package com.latticeengines.release.hipchat.activities;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import com.latticeengines.release.error.handler.ErrorHandler;
import com.latticeengines.release.exposed.activities.BaseActivity;
import com.latticeengines.release.exposed.domain.StatusContext;
import com.latticeengines.release.hipchat.service.HipChatService;

@Component("finishReleaseNotificationActivity")
public class FinishReleaseNotificationActivity extends BaseActivity {

    @Autowired
    private HipChatService hipchatService;

    @Value("${release.hipchat.url}")
    private String url;

    @Autowired
    public FinishReleaseNotificationActivity(@Qualifier("defaultErrorHandler") ErrorHandler errorHandler) {
        super(errorHandler);
    }

    @Override
    public StatusContext runActivity() {
        ResponseEntity<String> response = hipchatService.sendNotification(url, "green", "Release Process Finished!");
        statusContext.setStatusCode(response.getStatusCode().value());
        return statusContext;
    }

}
