package com.latticeengines.security.exposed.service;

import java.util.Map;

import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.security.exposed.AccessLevel;

public interface InternalTestUserService {
    void createUser(String username, String email, String firstName, String lastName);

    void createUser(String username, String email, String firstName, String lastName, String password);

    Ticket loginCreds(String username, String password);

    void deleteUserWithUsername(String username);

    Map<AccessLevel, User> createAllTestUsersIfNecessaryAndReturnStandardTestersAtEachAccessLevel();

    void logoutTicket(Ticket ticket);

    String getUsernameForAccessLevel(AccessLevel accessLevel);

    String getGeneralPassword();


}
