package com.latticeengines.oauth2.service.impl;

import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.oauth2.dao.UserDao;
import com.latticeengines.oauth2.dao.impl.UserDaoImpl;
import com.latticeengines.oauth2.service.UserService;

@Component
public class UserServiceImpl implements UserService {

    @Autowired
    private DataSource dataSource;

    @Override
    public List<Map<String, Object>> findByUserName(String userName) {

        NamedParameterJdbcTemplate template = new NamedParameterJdbcTemplate(dataSource);
        UserDao userDao = new UserDaoImpl(template);
        return userDao.getUserByName(userName);
    }
}
