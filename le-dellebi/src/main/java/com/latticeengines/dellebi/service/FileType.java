package com.latticeengines.dellebi.service;

public enum FileType {

    QUOTE("QuoteTransDailyFlow"), ORDER_SUMMARY("OrderSumDailyFlow"), ORDER_DETAIL(
            "OrderDetailDailyFlow"), SHIP("ShipDailyFlow"), WARRANTE("WarrantyDailyFlow");

    private String type;

    private FileType(String type) {
        this.type = type;
    }

    public String getType() {
        return this.type;
    }
}
