package com.latticeengines.domain.exposed.dante;

public enum TalkingPointAttributeNotion {
    Account("account"), //
    Recommendation("recommendation"), //
    Variable("variable");

    private String notion;

    TalkingPointAttributeNotion(String notion) {
        this.notion = notion;
    }

    public String getNotion() {
        return notion;
    }

    public static boolean isValidDanteNotion(String notion) {
        for (TalkingPointAttributeNotion v : values())
            if (v.getNotion().equalsIgnoreCase(notion))
                return true;
        return false;
    }

    public static TalkingPointAttributeNotion getDanteNotion(String notion) {
        for (TalkingPointAttributeNotion v : values())
            if (v.getNotion().equalsIgnoreCase(notion))
                return v;
        throw new IllegalArgumentException();
    }
}