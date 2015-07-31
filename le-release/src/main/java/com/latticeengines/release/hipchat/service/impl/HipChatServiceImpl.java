package com.latticeengines.release.hipchat.service.impl;

import java.util.HashMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.release.hipchat.service.HipChatService;

@Service("hipchatService")
public class HipChatServiceImpl implements HipChatService{
    
    @Value("${release.hipchat.url}")
    private String url;

    @Autowired
    private RestTemplate restTemplate;

    @Override
    public void sendNotification(String color, String message) {
        Notification notification = new Notification(color, message);
        restTemplate.postForObject(url, notification, String.class, new HashMap<String, String>());
    }

    private static class Notification{

        private String color = "green";
        private String messageFormat = "text";
        private String message;

        public Notification(String color, String message){
            this.color = color;
            this.message = message;
        }

        @JsonProperty("color")
        public String getColor(){
            return this.color;
        }

        @JsonProperty("color")
        public void setColor(String color){
            this.color = color;
        }

        @JsonProperty("message_format")
        public String getMessageFormat(){
            return this.messageFormat;
        }

        @JsonProperty("message_format")
        public void setMessageFormat(String messageFormat){
            this.messageFormat = messageFormat;
        }

        @JsonProperty("message")
        public String getMessage(){
            return this.message;
        }

        @JsonProperty("message")
        public void setMessage(String message){
            this.message = message;
        }
    }

}
