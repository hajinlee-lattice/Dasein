package com.latticeengines.scoringapi.score.impl;

import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class LatticeIdGenerator {
    private static ObjectMapper mapper = new ObjectMapper();

    public static String generateLatticeId(Map<String, Object> attributeValues) {
        // this is only temporary implementation and it will be replaced when
        // lattice id txn is merged
        try {
            String serializedValue = mapper.writeValueAsString(attributeValues);
            return DigestUtils.shaHex(serializedValue);
        } catch (JsonProcessingException e) {
            return null;
        }
    }

}
