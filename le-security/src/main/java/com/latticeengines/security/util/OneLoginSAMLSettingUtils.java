package com.latticeengines.security.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.onelogin.saml2.exception.Error;
import com.onelogin.saml2.settings.Saml2Settings;
import com.onelogin.saml2.settings.SettingsBuilder;

public final class OneLoginSAMLSettingUtils {

    protected OneLoginSAMLSettingUtils() {
        throw new UnsupportedOperationException();
    }

    private static final Logger log = LoggerFactory.getLogger(OneLoginSAMLSettingUtils.class);

    private static final String SP_SLO_URL = "onelogin.saml2.sp.single_logout_service.url";
    private static final String LOGIN_URL = "onelogin.saml2.sp.login.url";

    public static Saml2Settings buildSettings(String profile) {
        try {
            Properties idpProps = loadProfile(profile);
            return new SettingsBuilder() //
                    .fromFile("onelogin/base.properties") //
                    .fromProperties(idpProps) //
                    .build();
        } catch (Error e) {
            throw new RuntimeException(e);
        }
    }

    public static String getLoginUrl(String profile) {
        Properties props = loadProfile(profile);
        return props.getProperty(LOGIN_URL);
    }

    public static boolean isSLOEnabled(String profile) {
        Properties props = loadProfile(profile);
        return StringUtils.isNotBlank(props.getProperty(SP_SLO_URL));
    }

    private static Properties loadProfile(String profile) {
        String resource = String.format("onelogin/profile_%s.properties", profile);
        InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
        Properties prop = new Properties();
        try {
            if (inputStream != null) {
                prop.load(inputStream);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load idp settings from " + resource, e);
        }
        return prop;
    }

}
