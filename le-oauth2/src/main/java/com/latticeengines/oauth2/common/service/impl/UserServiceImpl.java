package com.latticeengines.oauth2.common.service.impl;

import javax.annotation.Resource;
import javax.sql.DataSource;

import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.provisioning.JdbcUserDetailsManager;
import org.springframework.stereotype.Component;

import com.latticeengines.oauth2.common.service.UserService;

@Component
public class UserServiceImpl implements UserService {

    @Resource(name = "dataSourceOauth2")
    private DataSource dataSource;

    private JdbcUserDetailsManager jdbcManager = new JdbcUserDetailsManager();

    @Override
    public UserDetails findByUserName(String userName) {

        jdbcManager.setDataSource(dataSource);
        return jdbcManager.loadUserByUsername(userName);
    }
}
