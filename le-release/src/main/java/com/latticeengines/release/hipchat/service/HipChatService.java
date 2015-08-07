package com.latticeengines.release.hipchat.service;

import org.springframework.http.ResponseEntity;

public interface HipChatService {

    ResponseEntity<String> sendNotification(String url, String color, String message);

}
