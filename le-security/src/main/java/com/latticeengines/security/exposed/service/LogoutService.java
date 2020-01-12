package com.latticeengines.security.exposed.service;

public interface LogoutService {

    // return a redirect url if necessary
    String logout(String token, String redirectTo);

}
