package com.latticeengines.datacloud.core.util;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.util.JsonUtils;

public class SlackUtils {

    private static final Logger log = LoggerFactory.getLogger(SlackUtils.class);

    private static final String SLACK_COLOR_DANGER = "danger";
    private static final ObjectMapper OM = new ObjectMapper();
    private static RestTemplate slackRestTemplate = HttpClientUtils.newSSLEnforcedRestTemplate();

    public static void sendSlack(String slackWebHookUrl, String title, String text, String color, String bot,
            String pretext) {
        try {
            String payload = slackPayload(title, text, color, bot, pretext);
            slackRestTemplate.postForObject(slackWebHookUrl, payload, String.class);
        } catch (Exception e) {
            log.error("Failed to send slack message.", e);
        }
    }

    private static String slackPayload(String title, String text, String color, String bot, String pretext) {
        ObjectNode objectNode = OM.createObjectNode();
        objectNode.put("username", bot);
        ArrayNode attachments = OM.createArrayNode();
        ObjectNode attachment = OM.createObjectNode();
        if (SLACK_COLOR_DANGER.equals(color)) {
            pretext = "<!channel> " + pretext;
        }
        attachment.put("pretext", pretext);
        if (StringUtils.isNotEmpty(color)) {
            attachment.put("color", color);
        }
        if (StringUtils.isNotEmpty(title)) {
            attachment.put("title", title);
        }
        attachment.put("text", text);
        attachments.add(attachment);
        objectNode.put("attachments", attachments);
        return JsonUtils.serialize(objectNode);
    }
}
