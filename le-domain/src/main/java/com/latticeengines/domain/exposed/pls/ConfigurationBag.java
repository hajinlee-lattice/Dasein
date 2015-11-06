package com.latticeengines.domain.exposed.pls;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;

public class ConfigurationBag<T extends HasOptionAndValue> {
    protected List<T> bag;

    public ConfigurationBag(List<T> bag) {
        this.bag = bag;
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
            return Double.parseDouble(inner.getValue());
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to parse option %s as a double", optionName));
        }
    }

    public void setString(String optionName, String value) {
        try {
            T inner = createIfNecessary(optionName);
            inner.setOption(optionName);
            inner.setValue(value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void setInt(String optionName, int value) {
        try {
            T inner = createIfNecessary(optionName);
            inner.setOption(optionName);
            inner.setValue(Integer.toString(value));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void setDouble(String optionName, double value) {
        try {
            T inner = createIfNecessary(optionName);
            inner.setOption(optionName);
            inner.setValue(Double.toString(value));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private T createIfNecessary(String optionName) {
        try {
            T option = findOption(optionName);
            if (option == null) {
                option = classT().newInstance();
                bag.add(option);
            }
            return option;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private T findOption(String optionName) {
        for (T inner : bag) {
            if (inner.getOption().equals(optionName)) {
                return inner;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private Class<T> classT() {
        Type[] typeArguments = ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments();
        Type type = typeArguments[0];
        return (Class<T>) type;
    }
}
