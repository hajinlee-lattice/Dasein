package com.latticeengines.release.hipchat.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import com.latticeengines.release.exposed.domain.Notification;
import com.latticeengines.release.hipchat.service.HipChatService;

@Service("hipchatService")
public class HipChatServiceImpl implements HipChatService{

    @Autowired
    private RestTemplate restTemplate;

    @Override
    public ResponseEntity<String> sendNotification(String url, String color, String message) {
        Notification notification = new Notification(color, message);
        return restTemplate.postForEntity(url, notification, String.class);
    }
}
