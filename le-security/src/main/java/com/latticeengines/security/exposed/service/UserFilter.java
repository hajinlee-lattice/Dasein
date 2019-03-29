package com.latticeengines.security.exposed.service;

import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.security.exposed.AccessLevel;

public interface UserFilter {

    static final UserFilter TRIVIAL_FILTER = new UserFilter() {
        @Override
        public boolean visible(User user) {
            if (AccessLevel.INTERNAL_ADMIN.name().equals(user.getAccessLevel())
                    || AccessLevel.INTERNAL_USER.name().equals(user.getAccessLevel())
                    || AccessLevel.SUPER_ADMIN.name().equals(user.getAccessLevel())) {
                return user.getExpirationDate() == null ? true : user.getExpirationDate() > System.currentTimeMillis();
            } else {
                return true;
            }
        }
    };

    boolean visible(User user);


}
