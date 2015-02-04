package com.latticeengines.scoringharness.marketoharness;

import java.util.HashMap;
import java.util.Map;

/**
 * Unfortunately, Marketo does not support a REST api for retrieving a lead by
 * email address. As a workaround to using the Marketo SOAP API, cache ids of
 * all leads written to marketo and retrieve them using this cache.
 */
public class MarketoLeadCache {
    private static MarketoLeadCache s_instance;

    private Map<String, String> dictionary = new HashMap<String, String>();

    public static MarketoLeadCache instance() {
        return s_instance == null ? s_instance = new MarketoLeadCache() : s_instance;
    }

    public void put(String externalId, String marketoId) {
        if (marketoId == null || externalId == null) {
            throw new IllegalArgumentException("marketoId and externalId must be non-null");
        }
        dictionary.put(externalId, marketoId);
    }

    public String get(String externalId) {
        if (externalId == null) {
            throw new IllegalArgumentException("externalId must be non-null");
        }
        if (dictionary.containsKey(externalId)) {
            return dictionary.get(externalId);
        }
        return null;
    }
}
