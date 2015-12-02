package com.latticeengines.domain.exposed.propdata;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

public class MatchClientDeserializer extends JsonDeserializer<MatchClient> {

    @Override
    public MatchClient deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException {
        JsonNode jNode = jsonParser.readValueAs(JsonNode.class);
        if (jNode.has("Name")) {
            String name = jNode.get("Name").asText();
            MatchClient client = MatchClient.valueOf(name);
            if (client == null) {
                throw new IOException("Cannot recognize MatchClient " + name);
            }
            return client;
        }
        throw new IOException("Cannot find the Name field in json " + jNode);
    }
}