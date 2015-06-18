package com.latticeengines.oauth2.service;

import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;

public class OAuthUser extends User implements UserDetails {

    private static final long serialVersionUID = 242742594459972345L;

    public OAuthUser(String userName, String password, Boolean enabled) {
        super(userName, password, enabled, true, true, true, AuthorityUtils
                .commaSeparatedStringToAuthorityList("ROLE_USER"));
    }

}