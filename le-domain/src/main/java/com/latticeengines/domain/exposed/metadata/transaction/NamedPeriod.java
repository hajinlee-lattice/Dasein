package com.latticeengines.domain.exposed.metadata.transaction;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public enum NamedPeriod {
    HASEVER("HasEver"), //
    LASTQUARTER("LasterQuarter");

    private final String name;
    private final String MIN_DATE = "1900-01-01";
    private final String MAX_DATE = "2400-12-31";

    private static Map<String, NamedPeriod> nameMap;

    static {
        nameMap = new HashMap<>();
        for (NamedPeriod period : NamedPeriod.values()) {
            nameMap.put(period.getName(), period);
        }
    }

    NamedPeriod(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public String toString() {
        return this.name;
    }

    public String[] getTimeRange() {
        String[] timeRange = new String[2];
        LocalDate today = LocalDate.now();
        int month = today.getMonthValue();
        int year = today.getYear();
        switch (this) {
            case HASEVER:
                timeRange[0] = MIN_DATE;
                timeRange[1] = MAX_DATE;
                break;
            case LASTQUARTER:
                if (month <= 3) {
                   timeRange[0] = (year - 1) + "-10-01";
                   timeRange[1] = (year - 1) + "-12-31";
                } else {
                   int lastQuarter = (month -3 ) / 3; 
                   timeRange[0] = year + "-" + getMonthStr(lastQuarter * 3 + 1) + "-01";
                   timeRange[1] = year + "-" + getMonthStr(lastQuarter * 3 + 3) + "-31";
                }
                break;
        }
        return timeRange;
    }

    private String getMonthStr(int month) {
        return ((month < 10) ?  "0" + month : "" + month);
    }

    public static Set<String> availableNames() {
        return new HashSet<>(nameMap.keySet());
    }

    public static NamedPeriod fromName(String name) {
        if (nameMap.containsKey(name)) {
            return nameMap.get(name);
        } else {
            throw new IllegalArgumentException("Cannot find a NamedPeriod with name " + name);
        }
    }

}
