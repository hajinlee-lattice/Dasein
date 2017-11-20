package com.latticeengines.domain.exposed.datacloud.check;

//@formatter:off
public enum CheckCode {

    DuplicatedValue("Found duplicated value [%s] in the field [%s].");

    private final String msgPattern;

    CheckCode(String msgPattern) {
        this.msgPattern = msgPattern;
    }

    public String getMessage(Object... vals) {
        return String.format(this.msgPattern, vals);
    }

}
