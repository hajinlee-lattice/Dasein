package com.latticeengines.dellebi.service;

public enum FileType {

    QUOTE("Quote"), //
    ORDER_SUMMARY("Order_Summary"), //
    ORDER_DETAIL("Order_Detail"), //
    SHIP("Ship"), //
    WARRANTY("Warranty"), //
    SKU_GLOBAL("SKU_Global"), //
    SKU_MANUFACTURER("SKU_Manufacturer"), //
    SKU_ITM_CLS_CODE("SKU_Itm_Cls_Code"), //
    CALENDAR("Calendar"), //
    CHANNEL("Channel");

    private String type;

    private FileType(String type) {
        this.type = type;
    }

    public String getType() {
        return this.type;
    }
}
