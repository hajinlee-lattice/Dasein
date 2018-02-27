package com.latticeengines.domain.exposed.metadata;

import org.apache.commons.lang3.StringUtils;

public interface Named {

    default String getName() {
        String name = getClass().getSimpleName();
        if (StringUtils.isBlank(name)) {
            return getClass().getName().substring(getClass().getName().lastIndexOf(".") + 1);
        } else {
            return name;
        }
    }

}
