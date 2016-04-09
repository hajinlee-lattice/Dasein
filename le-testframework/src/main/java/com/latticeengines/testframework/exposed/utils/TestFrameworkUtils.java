package com.latticeengines.testframework.exposed.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.security.exposed.AccessLevel;

public class TestFrameworkUtils {
    public static final String GENERAL_PASSWORD = "admin";
    public static final String GENERAL_PASSWORD_HASH = "EETAlfvFzCdm6/t3Ro8g89vzZo6EDCbucJMTPhYgWiE=";

    public static final String PASSWORD_TESTER = "pls-password-tester@test.lattice-engines.ext";
    public static final String PASSWORD_TESTER_PASSWORD = "Lattice123";
    public static final String PASSWORD_TESTER_PASSWORD_HASH = "3OCRIbECCiTtJ8FyaNgvTjNES/eyjQUK59Z5rMCnrAk=";

    public static final String TESTING_USER_FIRST_NAME = "Lattice";
    public static final String TESTING_USER_LAST_NAME = "Tester";
    public static final String SUPER_ADMIN_USERNAME = "pls-super-admin-tester@test.lattice-engines.com";
    public static final String INTERNAL_ADMIN_USERNAME = "pls-internal-admin-tester@test.lattice-engines.com";
    public static final String INTERNAL_USER_USERNAME = "pls-internal-user-tester@test.lattice-engines.com";
    public static final String EXTERNAL_ADMIN_USERNAME = "pls-external-admin-tester@test.lattice-engines.ext";
    public static final String EXTERNAL_USER_USERNAME = "pls-external-user-tester@test.lattice-engines.ext";
    public static final String EXTERNAL_USER_USERNAME_1 = "pls-external-user-tester-1@test.lattice-engines.ext";
    public static final String THIRD_PARTY_USER_USERNAME = "pls-third-party-user-tester@test.lattice-engines.ext";

    public static final String AD_USERNAME = "testuser1";
    public static final String AD_PASSWORD = "Lattice1";

    public static final String PD_TENANT_REG_PREFIX = "pd";
    public static final String LP3_TENANT_REG_PREFIX = "lp3";

    public static final String TENANTID_PREFIX = "LETest";

    public static String usernameForAccessLevel(AccessLevel accessLevel) {
        switch (accessLevel) {
            case SUPER_ADMIN:
                return TestFrameworkUtils.SUPER_ADMIN_USERNAME;
            case INTERNAL_ADMIN:
                return TestFrameworkUtils.INTERNAL_ADMIN_USERNAME;
            case INTERNAL_USER:
                return TestFrameworkUtils.INTERNAL_USER_USERNAME;
            case EXTERNAL_ADMIN:
                return TestFrameworkUtils.EXTERNAL_ADMIN_USERNAME;
            case EXTERNAL_USER:
                return TestFrameworkUtils.EXTERNAL_USER_USERNAME;
            case THIRD_PARTY_USER:
                return TestFrameworkUtils.THIRD_PARTY_USER_USERNAME;
            default:
                throw new IllegalArgumentException("Unknown access level!");
        }
    }

    public static UserRegistration createUserRegistration(AccessLevel accessLevel) {
        String username = usernameForAccessLevel(accessLevel);
        User user = new User();
        user.setEmail(username);
        user.setFirstName(TestFrameworkUtils.TESTING_USER_FIRST_NAME);
        user.setLastName(TestFrameworkUtils.TESTING_USER_LAST_NAME);

        Credentials credentials = new Credentials();
        credentials.setUsername(user.getEmail());
        credentials.setPassword(GENERAL_PASSWORD_HASH);

        user.setUsername(credentials.getUsername());

        UserRegistration userRegistration = new UserRegistration();
        userRegistration.setUser(user);
        userRegistration.setCredentials(credentials);

        return userRegistration;
    }

    public static Boolean isTestTenant(Tenant tenant) {
        String tenantId = CustomerSpace.parse(tenant.getId()).getTenantId();
        Pattern pattern = Pattern.compile(TENANTID_PREFIX + "\\d+");
        Matcher matcher = pattern.matcher(tenantId);
        return matcher.find();
    }

}
