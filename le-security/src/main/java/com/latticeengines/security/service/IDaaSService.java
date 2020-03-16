package com.latticeengines.security.service;

import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.security.service.impl.IDaaSUser;

public interface IDaaSService {

    LoginDocument login(Credentials credentials);

    IDaaSUser getIDaaSUser(String email);

    IDaaSUser createIDaaSUser(IDaaSUser user);

    IDaaSUser updateIDaaSUser(IDaaSUser user);



}
