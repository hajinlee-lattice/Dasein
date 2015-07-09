package com.latticeengines.security.exposed.service;

import com.latticeengines.domain.exposed.security.User;

public interface UserFilter {

    static final UserFilter TRIVIAL_FILTER = new UserFilter() {
        @Override
        public boolean visible(User user) {
            return true;
        }
    };

    boolean visible(User user);


}
