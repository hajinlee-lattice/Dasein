package com.latticeengines.oauth2.dao;

import java.util.List;
import java.util.Map;

import com.latticeengines.db.exposed.dao.GenericDao;

public interface UserDao extends GenericDao {

    List<Map<String, Object>> getUserByName(String userName);

  
}
