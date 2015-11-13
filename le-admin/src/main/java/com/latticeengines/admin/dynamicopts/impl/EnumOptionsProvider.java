package com.latticeengines.admin.dynamicopts.impl;

import java.util.ArrayList;
import java.util.List;

import com.latticeengines.admin.dynamicopts.OptionsProvider;

public class EnumOptionsProvider implements OptionsProvider {

    private final List<String> options = new ArrayList<>();

    public EnumOptionsProvider(Class<? extends Enum<?>> enumClz) {
        for (Enum<?> item : enumClz.getEnumConstants()) {
            options.add(item.toString());
        }
    }

    @Override
    public List<String> getOptions() {
        return options;
    }
}
