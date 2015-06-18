package com.latticeengines.oauth2.service.impl;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
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

        List<Map<String, Object>> users = userService.findByUserName(userName);
        if (CollectionUtils.isEmpty(users)) {
            throw new LedpException(LedpCode.LEDP_23000, new String[] { userName });
        }

        if (users.size() > 1) {
            throw new LedpException(LedpCode.LEDP_23001, new String[] { userName });
        }
        return new OAuthUser((String) users.get(0).get("username"), (String) users.get(0).get("password"),
                (Boolean) users.get(0).get("enabled"));
    }

}
