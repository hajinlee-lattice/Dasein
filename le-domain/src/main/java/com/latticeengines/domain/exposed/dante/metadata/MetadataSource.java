package com.latticeengines.domain.exposed.dante.metadata;

public enum MetadataSource {
    BaseInstallation(0), VisiDB(1), DataCloud(2);

    private final int value;

    MetadataSource(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
