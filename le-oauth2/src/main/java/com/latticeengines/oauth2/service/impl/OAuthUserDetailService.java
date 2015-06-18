package com.latticeengines.oauth2.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.oauth2.service.OAuthUser;
import com.latticeengines.oauth2.service.UserService;

@Component
public class OAuthUserDetailService implements UserDetailsService {

    @Autowired
    private UserService userService;

    @Override
    public UserDetails loadUserByUsername(String userName) throws UsernameNotFoundException {

        UserDetails userDetails = userService.findByUserName(userName);
        if (userDetails == null) {
            throw new LedpException(LedpCode.LEDP_23000, new String[] { userName });
        }

        return new OAuthUser(userDetails);
    }

}
