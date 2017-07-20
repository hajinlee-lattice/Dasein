package com.latticeengines.domain.exposed.playmakercore;

public enum SynchronizationDestinationEnum {
    SFDC(0), MAP(1), SFDC_AND_MAP(2);

    private int type;

    private SynchronizationDestinationEnum(int intType) {
        this.type = intType;
    }

    public static int mapToIntType(String strType) {
        strType = strType.toUpperCase();
        for (SynchronizationDestinationEnum type : values()) {
            if (type.name().equals(strType)) {
                return type.type;
            }
        }
        return SFDC.type;
    }

    public static SynchronizationDestinationEnum fromIntValue(int intVal) {
        for (SynchronizationDestinationEnum en : values()) {
            if (en.type == intVal) {
                return en;
            }
        }
        return SFDC;
    }
}
