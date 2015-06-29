package com.latticeengines.playmaker.service.impl;

import org.joda.time.DateTime;
import org.springframework.security.oauth2.provider.ClientDetails;

/**
 * ExtendedClientDetails adds client secret expiration functionality to typical
 * OAuth2 client details.
 */
public interface ExtendedClientDetails extends ClientDetails {

    DateTime getClientSecretExpiration();
}
