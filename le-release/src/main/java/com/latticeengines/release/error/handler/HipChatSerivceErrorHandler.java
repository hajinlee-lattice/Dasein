package com.latticeengines.release.error.handler;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.release.exposed.domain.ProcessContext;
import com.latticeengines.release.hipchat.service.HipChatService;

@Component("hipchatServiceErrorHandler")
public class HipChatSerivceErrorHandler implements ErrorHandler{

    @Autowired
    private HipChatService hipchatService;

    @Override
    public void handleError(ProcessContext context, Throwable th) {
        //hipchatService.sendNotification("red", "Release Process Failed!");
    }

}
