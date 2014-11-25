package com.latticeengines.skald.model;

import java.util.HashMap;

// First keyed by model name, then by tag, mapping to a model version number.
@SuppressWarnings("serial")
public class ModelTags extends HashMap<String, HashMap<String, Integer>> {
    public static String ACTIVE = "active";
    public static String TEST = "test";
}
