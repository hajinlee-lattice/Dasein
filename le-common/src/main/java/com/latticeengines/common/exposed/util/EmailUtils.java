package com.latticeengines.common.exposed.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringEscapeUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class EmailUtils {

    public static List<String> parseEmails(String emailsInJson) {
        List<String> adminEmails = new ArrayList<>();

        try {
            ObjectMapper mapper = new ObjectMapper();
            String unescaped = StringEscapeUtils.unescapeJava(emailsInJson);
            JsonNode aNode = mapper.readTree(unescaped);
            if (!aNode.isArray()) {
                throw new IOException("AdminEmails suppose to be a list of strings");
            }
            for (JsonNode node : aNode) {
                adminEmails.add(node.asText());
            }
        } catch (IOException e) {
            throw new RuntimeException(String.format("Cannot parse AdminEmails to a list of valid emails: %s",
                    emailsInJson), e);
        }

        return adminEmails;
    }

}
