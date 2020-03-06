package com.latticeengines.security.exposed.service;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.security.exposed.AccessLevel;

public interface UserFilter {

    UserFilter EXTERNAL_FILTER = user -> {
        if (StringUtils.isEmpty(user.getAccessLevel())) {
            return false;
        }
        AccessLevel level = AccessLevel.valueOf(user.getAccessLevel());
        return level.equals(AccessLevel.EXTERNAL_USER) || level.equals(AccessLevel.EXTERNAL_ADMIN);
    };

    UserFilter TRIVIAL_FILTER = user -> {
        if (AccessLevel.INTERNAL_ADMIN.name().equals(user.getAccessLevel())
                || AccessLevel.INTERNAL_USER.name().equals(user.getAccessLevel())
                || AccessLevel.SUPER_ADMIN.name().equals(user.getAccessLevel())) {
            return user.getExpirationDate() == null || user.getExpirationDate() > System.currentTimeMillis();
        } else {
            return true;
        }
    };

    boolean visible(User user);


}
