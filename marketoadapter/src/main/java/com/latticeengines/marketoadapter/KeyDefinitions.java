package com.latticeengines.marketoadapter;

import java.util.HashMap;

import com.latticeengines.domain.exposed.camille.CustomerSpace;

@SuppressWarnings("serial")
public class KeyDefinitions extends HashMap<String, KeyDefinitions.Value> {
    public static class Value {
        public String destination;
        public CustomerSpace space;
    }
}