package com.latticeengines.domain.exposed.metadata;

import java.util.HashMap;
import java.util.Map;

public enum Category {
    FIRMOGRAPHICS("Firmographics"), //
    GROWTH_TRENDS("Growth Trends"), //
    INTENT("Intent"), //
    LEAD_INFORMATION("Lead Information"), //
    ONLINE_PRESENCE("Online Presence"), //
    TECHNOLOGY_PROFILE("Technology Profile"), //
    WEBSITE_KEYWORDS("Website Keywords"), //
    WEBSITE_PROFILE("Website Profile");

    private final String name;
    private static Map<String, Category> nameMap;

    static {
        nameMap = new HashMap<>();
        for (Category category: Category.values()) {
            nameMap.put(category.getName(), category);
        }
    }

    Category(String name) {
        this.name = name;
    }

    public String getName() { return this.name; }

    public static Category fromName(String name) {
        if (nameMap.containsKey(name)) {
            return nameMap.get(name);
        } else  {
            throw new IllegalArgumentException("Cannot find a Category with name " + name);
        }
    }

}