package com.latticeengines.monitor.exposed.service.impl;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.monitor.SlackSettings;
import com.latticeengines.monitor.exposed.service.SlackService;

@Component("SlackService")
public class SlackServiceImpl implements SlackService {

    private static final Logger log = LoggerFactory.getLogger(SlackServiceImpl.class);
    private ObjectMapper OM = new ObjectMapper();
    private RestTemplate slackRestTemplate = HttpClientUtils.newSSLEnforcedRestTemplate();

    @Override
    public void sendSlack(SlackSettings settings) {
        try {
            String payload = slackPayload(settings);
            slackRestTemplate.postForObject(settings.getWebHookUrl(), payload, String.class);
        } catch (Exception e) {
            log.error("Failed to send slack message.", e);
        }
    }

    private String slackPayload(SlackSettings settings) {
        ObjectNode objectNode = OM.createObjectNode();
        objectNode.put("username", settings.getUserName());
        ArrayNode attachments = OM.createArrayNode();
        ObjectNode attachment = OM.createObjectNode();
        if (settings.getColor() == SlackSettings.Color.DANGER) {
            attachment.put("pretext", "<!channel> " + settings.getPretext());
        } else {
            attachment.put("pretext", settings.getPretext());
        }
        if (settings.getColor() != null) {
            attachment.put("color", settings.getColor().getColor());
        }
        if (StringUtils.isNotEmpty(settings.getTitle())) {
            attachment.put("title", settings.getTitle());
        }
        attachment.put("text", settings.getText());
        attachments.add(attachment);
        objectNode.set("attachments", attachments);
        return JsonUtils.serialize(objectNode);
    }

}
