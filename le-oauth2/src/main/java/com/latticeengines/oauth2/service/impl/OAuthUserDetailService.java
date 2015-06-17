package com.latticeengines.oauth2.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Component;

import com.latticeengines.oauth2.service.UserService;

@Component
public class OAuthUserDetailService implements UserDetailsService {

    @Autowired
    private UserService userService; // Repository methods are here

    @Override
    public UserDetails loadUserByUsername(String userName) throws UsernameNotFoundException {
        User user = userService.findByUserName(userName);
        if (user == null) {
            throw new UsernameNotFoundException("UserName " + userName + " not found");
        }
        return new OAuthUser(user.getUsername(), user.getPassword());
    }

}
