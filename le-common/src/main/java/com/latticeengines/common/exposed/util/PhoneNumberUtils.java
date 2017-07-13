package com.latticeengines.common.exposed.util;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.i18n.phonenumbers.NumberParseException;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.Phonenumber.PhoneNumber;

public class PhoneNumberUtils {
    private static final Logger log = LoggerFactory.getLogger(PhoneNumberUtils.class);
    public static String getStandardPhoneNumber(String phoneNumber, String countryCode) {
        phoneNumber = LocationStringStandardizationUtils.getStandardString(phoneNumber);
        if (StringUtils.isEmpty(countryCode)) {
            countryCode = LocationUtils.US;
        }
        if (StringUtils.isEmpty(phoneNumber)) {
            return null;
        }
        PhoneNumberUtil phoneUtil = PhoneNumberUtil.getInstance();
        try {
            PhoneNumber standardPhoneNumber = phoneUtil.parse(phoneNumber, countryCode);
            return String.valueOf(standardPhoneNumber.getCountryCode())
                    + String.valueOf(standardPhoneNumber.getNationalNumber());
        } catch (NumberParseException e) {
            if (phoneNumber != null) {
                log.info("Fail to standardize phone number: " + phoneNumber);
            }
            return phoneNumber;
        }
    }
}
