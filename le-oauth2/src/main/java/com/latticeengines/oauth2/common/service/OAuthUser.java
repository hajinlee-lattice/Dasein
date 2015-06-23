package com.latticeengines.oauth2.common.service;

import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;

public class OAuthUser extends User implements UserDetails {

    private static final long serialVersionUID = 242742594459972345L;

    public OAuthUser(UserDetails userDetails) {
        super(userDetails.getUsername(), userDetails.getPassword(), userDetails.isEnabled(), true, true, true,
                userDetails.getAuthorities());

    }
}