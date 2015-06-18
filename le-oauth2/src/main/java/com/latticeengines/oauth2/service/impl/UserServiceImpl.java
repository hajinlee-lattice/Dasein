package com.latticeengines.oauth2.service.impl;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.provisioning.JdbcUserDetailsManager;
import org.springframework.stereotype.Component;

import com.latticeengines.oauth2.service.UserService;

@Component
public class UserServiceImpl implements UserService {

    @Autowired
    private DataSource dataSource;

    @Autowired
    private JdbcUserDetailsManager jdbcManager;

    @Override
    public UserDetails findByUserName(String userName) {

        jdbcManager.setDataSource(dataSource);
        return jdbcManager.loadUserByUsername(userName);
    }
}
