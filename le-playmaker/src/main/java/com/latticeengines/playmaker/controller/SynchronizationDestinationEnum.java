package com.latticeengines.playmaker.controller;

enum SynchronizationDestinationEnum {
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
}
