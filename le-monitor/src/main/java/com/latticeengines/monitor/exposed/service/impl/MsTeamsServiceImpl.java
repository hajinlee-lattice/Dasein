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
import com.latticeengines.domain.exposed.monitor.MsTeamsSettings;
import com.latticeengines.monitor.exposed.service.MsTeamsService;

@Component("MsTeamsService")
public class MsTeamsServiceImpl implements MsTeamsService {
    private static final Logger log = LoggerFactory.getLogger(MsTeamsServiceImpl.class);
    private ObjectMapper OM = new ObjectMapper();
    private RestTemplate msTeamsRestTemplate = HttpClientUtils.newSSLEnforcedRestTemplate();

    private String msTeamsPayload(MsTeamsSettings settings) {
        ObjectNode objectNode = OM.createObjectNode();
        ArrayNode sectionsNodeArr = OM.createArrayNode();
        ObjectNode sectionsNode = OM.createObjectNode();
        ArrayNode factsNodeArr = OM.createArrayNode();
        ObjectNode factsNode = OM.createObjectNode();
        if (settings.getColor() != null) {
            objectNode.put("themeColor", settings.getColor().getColor());
        }
        if (StringUtils.isNotEmpty(settings.getTitle())) {
            objectNode.put("title", settings.getTitle());
        }
        factsNode.put("name", "Environment");
        factsNode.put("value", settings.getPretext());
        objectNode.put("text", settings.getText());
        objectNode.put("@context", "https://schema.org/extensions");
        objectNode.put("@type", "MessageCard");
        factsNodeArr.add(factsNode);
        sectionsNode.set("facts", factsNodeArr);
        sectionsNodeArr.add(sectionsNode);
        objectNode.set("sections", sectionsNodeArr);
        return JsonUtils.serialize(objectNode);
    }

    @Override
    public void sendMsTeams(MsTeamsSettings settings) {
        try {
            String payload = msTeamsPayload(settings);
            msTeamsRestTemplate.postForObject(settings.getWebHookUrl(), payload, String.class);
        } catch (Exception e) {
            log.error("Failed to send msTeams message.", e);
        }
    }
}
