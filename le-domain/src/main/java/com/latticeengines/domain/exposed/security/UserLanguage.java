package com.latticeengines.domain.exposed.security;

public enum UserLanguage {
    Arabic, Chinese, English, French, Russian, Spanish;

    public static UserLanguage fromName(String language) {
        for (UserLanguage la : values()) {
            if (la.name().equalsIgnoreCase(language)) {
                return la;
            }
        }
        return null;
    }

}
