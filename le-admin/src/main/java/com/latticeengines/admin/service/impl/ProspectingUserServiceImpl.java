package com.latticeengines.admin.service.impl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.service.ProspectingUserService;
import com.latticeengines.domain.exposed.pls.RegistrationResult;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.monitor.exposed.service.EmailService;
import com.latticeengines.security.exposed.service.UserService;

@Component("prospectingUserService")
public class ProspectingUserServiceImpl implements ProspectingUserService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProspectingUserServiceImpl.class);

    @Value("${security.app.public.url:http://localhost:8081}")
    private String appPublicUrl;

    @Inject
    private EmailService emailService;

    @Inject
    private UserService userService;

    @Override
    public RegistrationResult createUser(UserRegistration userReg) {
        RegistrationResult result = userService.registerUserWithNoTenant(userReg);
        if (result.isValid()) {
            String tempPass = result.getPassword();
            emailService.sendNewUserEmail(userReg.getUser(), tempPass, appPublicUrl, false);
            LOGGER.info(String.format("%s registered as a new prevision user", userReg.getUser().getEmail()));
        } else {
            LOGGER.info(String.format("Failure of prevision user %s registration, user maybe exist",
                    userReg.getUser().getEmail()));
        }
        return result;
    }
}
