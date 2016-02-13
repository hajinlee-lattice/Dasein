package com.latticeengines.domain.exposed.scoringapi.unused;

import java.util.HashMap;

// There's a single instance of this document for each space, and it maps
// tags to version numbers for each model.
// First keyed by model name, then by tag, mapping to a model version number.
@SuppressWarnings("serial")
public class ModelTags extends HashMap<String, HashMap<String, Integer>> {
    public static String ACTIVE = "active";
    public static String TEST = "test";
}
