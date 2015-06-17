package com.latticeengines.oauth2.service.impl;

import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;

public class OAuthUser extends User implements UserDetails {

    private static final long serialVersionUID = 242742594459972345L;

    public OAuthUser(String userName, String password) {
        super(userName, password, AuthorityUtils.commaSeparatedStringToAuthorityList("ROLE_USER"));
    }

}