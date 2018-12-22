package com.latticeengines.datacloud.core.service;

import java.util.List;

import com.latticeengines.domain.exposed.monitor.SlackSettings;

public interface DataCloudNotificationService {
    void sendSlack(String title, String text, String slackBot, SlackSettings.Color color);

    void sendEmail(String subject, String content, List<String> recipients);
}
