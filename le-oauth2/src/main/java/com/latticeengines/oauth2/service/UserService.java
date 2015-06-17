package com.latticeengines.oauth2.service;

import org.springframework.security.core.userdetails.User;

public interface UserService {

    User findByUserName(String userName);

}
