package com.latticeengines.oauth2.service;

import java.util.List;
import java.util.Map;

public interface UserService {

    List<Map<String, Object>> findByUserName(String userName);

}
