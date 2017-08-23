package com.latticeengines.domain.exposed.dante;

public enum DanteAttributeNotion {
    Account("account"), //
    Recommendation("recommendation"), //
    Variable("variable");

    private String notion;

    DanteAttributeNotion(String notion) {
        this.notion = notion;
    }

    public String getNotion() {
        return notion;
    }

    public static boolean isValidDanteNotion(String notion) {
        for (DanteAttributeNotion v : values())
            if (v.getNotion().equalsIgnoreCase(notion))
                return true;
        return false;
    }

    public static DanteAttributeNotion getDanteNotion(String notion) {
        for (DanteAttributeNotion v : values())
            if (v.getNotion().equalsIgnoreCase(notion))
                return v;
        throw new IllegalArgumentException();
    }
}