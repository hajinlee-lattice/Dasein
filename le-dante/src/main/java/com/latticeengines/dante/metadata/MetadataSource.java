package com.latticeengines.dante.metadata;

public enum MetadataSource {
    BaseInstallation(0), VisiDB(1);

    private final int value;

    MetadataSource(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
