package com.latticeengines.domain.exposed.pls;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;

import javax.annotation.Nullable;

import com.google.common.base.Function;

public class ConfigurationBag<T extends HasOptionAndValue, E extends Enum<E>> {
    protected List<T> bag;

    public ConfigurationBag(List<T> bag) {
        this.bag = bag;
    }

    public <V> V get(E option, V dflt, final Class<V> clazz) {
        return get(option, dflt, clazz, new Function<String, V>() {
            @Nullable
            @Override
            public V apply(String input) {
                return parse(input, clazz);
            }
        });
    }

    public <V> V get(E option, final Class<V> clazz) {
        return get(option, clazz, new Function<String, V>() {

            @Nullable
            @Override
            public V apply(@Nullable String input) {
                return parse(input, clazz);
            }
        });
    }

    public <V> V get(E option, V dflt, Class<V> clazz, Function<String, V> parser) {
        HasOptionAndValue inner = findOption(option);
        if (inner == null) {
            return dflt;
        }
        return parser.apply(inner.getValue());
    }

    public <V> V get(E option, Class<V> clazz, Function<String, V> parser) {
        HasOptionAndValue inner = findOption(option);
        if (inner == null) {
            throw new RuntimeException(String.format("Required option %s was not specified", option));
        }
        return parser.apply(inner.getValue());
    }

    public <V> void set(E option, V value) {
        try {
            T inner = createIfNecessary(option);
            inner.setOption(option.toString());
            inner.setValue(value == null ? null : value.toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private <V> V parse(String string, Class<V> clazz) {
        try {
            if (string == null) {
                return null;
            }
            return clazz.getConstructor(new Class[] { String.class }).newInstance(string);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to parse %s as a %s", string, clazz.getSimpleName()));
        }
    }

    public String getString(E option, String dflt) {
        return get(option, dflt, String.class);
    }

    public int getInt(E option, int dflt) {
        return get(option, dflt, Integer.class);
    }

    public double getDouble(E option, double dflt) {
        return get(option, dflt, Double.class);
    }

    public boolean getBoolean(E option, boolean dflt) {
        return get(option, dflt, Boolean.class);
    }

    public String getString(E option) {
        return get(option, String.class);
    }

    public int getInt(E option) {
        return get(option, Integer.class);
    }

    public double getDouble(E option) {
        return get(option, Double.class);
    }

    public boolean getBoolean(E option) {
        return get(option, Boolean.class);
    }

    public void setString(E option, String value) {
        set(option, value);
    }

    public void setInt(E option, int value) {
        set(option, value);
    }

    public void setDouble(E option, double value) {
        set(option, value);
    }

    public void setBoolean(E option, boolean value) {
        set(option, value);
    }

    private T createIfNecessary(E option) {
        try {
            T optionAndValue = findOption(option);
            if (optionAndValue == null) {
                optionAndValue = classT().newInstance();
                bag.add(optionAndValue);
            }
            return optionAndValue;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private T findOption(E option) {
        for (T inner : bag) {
            if (inner.getOption().equals(option.toString())) {
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
