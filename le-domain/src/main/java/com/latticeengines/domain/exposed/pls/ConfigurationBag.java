package com.latticeengines.domain.exposed.pls;

import java.util.List;

public class ConfigurationBag {
    private List<HasOptionAndValue> bag;

    public ConfigurationBag(List<HasOptionAndValue> bag) {
        this.bag = bag;
    }

    public List<HasOptionAndValue> getBag() {
        return bag;
    }

    private HasOptionAndValue findOption(String optionName) {
        for (HasOptionAndValue inner : bag) {
            if (inner.getOption().equals(optionName)) {
                return inner;
            }
        }
        return null;
    }

    public String getString(String optionName, String dflt) {
        HasOptionAndValue inner = findOption(optionName);
        if (inner == null) {
            return dflt;
        }
        return inner.getValue();
    }

    public int getInt(String optionName, int dflt) {
        HasOptionAndValue inner = findOption(optionName);
        if (inner == null) {
            return dflt;
        }
        try {
            return Integer.parseInt(inner.getValue());
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to parse option %s as an int", optionName));
        }
    }

    public double getDouble(String optionName, double dflt) {
        HasOptionAndValue inner = findOption(optionName);
        if (inner == null) {
            return dflt;
        }
        try {
            return Double.parseDouble(optionName);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to parse option %s as a double", optionName));
        }
    }
}
