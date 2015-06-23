package com.latticeengines.oauth2.common.service;

import org.springframework.security.core.userdetails.UserDetails;

public interface UserService {

    UserDetails findByUserName(String userName);

}
