package com.latticeengines.datacloud.core.service.impl;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.service.DataCloudNotificationService;
import com.latticeengines.domain.exposed.monitor.MsTeamsSettings;
import com.latticeengines.domain.exposed.monitor.SlackSettings;
import com.latticeengines.domain.exposed.monitor.SlackSettings.Color;
import com.latticeengines.monitor.exposed.service.EmailService;
import com.latticeengines.monitor.exposed.service.MsTeamsService;
import com.latticeengines.monitor.exposed.service.SlackService;

@Component("dataCloudNotificationService")
public class DataCloudNotificationServiceImpl implements DataCloudNotificationService {

    private static final Logger log = LoggerFactory.getLogger(DataCloudNotificationServiceImpl.class);

    /***********************
     * Slack Configuration
     ***********************/
    @Inject
    private SlackService slackService;

    @Inject
    private MsTeamsService msTeamsService;

    @Value("${datacloud.slack.webhook.url}")
    private String slackWebHookUrl;

    @Value("${datacloud.msteams.webhook.url}")
    private String msteamsWebHookUrl;

    @Value("${common.le.environment}")
    private String leEnv;

    @Value("${common.le.stack}")
    private String leStack;

    /***********************
     * Email Configuration
     ***********************/
    @Inject
    private EmailService emailService;

    // If multiple, separated by ,
    @Value("${datacloud.email.recipients}")
    private String defaultRecipients;

    @Override
    public void sendSlack(String title, String text, String slackBot, Color color) {
        if (StringUtils.isNotEmpty(slackWebHookUrl)) {
            slackService.sendSlack(new SlackSettings(slackWebHookUrl, title, "[" + leEnv + "-" + leStack + "]", text,
                    slackBot, color));
        }
    }

    @Override
    public void sendEmail(String subject, String content, List<String> recipients) {
        if (CollectionUtils.isEmpty(recipients) && StringUtils.isNotBlank(defaultRecipients)) {
            recipients = Arrays.asList(defaultRecipients.split(","));
        }
        if (CollectionUtils.isNotEmpty(recipients)) {
            emailService.sendSimpleEmail(subject, content, "text/plain", recipients);
            log.info(String.format("Sent notification email to %s", String.join(",", recipients)));
        }
    }

    @Override
    public void sendMsTeams(String title, String text,
            com.latticeengines.domain.exposed.monitor.MsTeamsSettings.Color color) {
        if (StringUtils.isNotEmpty(msteamsWebHookUrl)) {
            msTeamsService.sendMsTeams(new MsTeamsSettings(msteamsWebHookUrl, title,
                    "[" + leEnv + "-" + leStack + "]", text, color));
        }
    }

}
