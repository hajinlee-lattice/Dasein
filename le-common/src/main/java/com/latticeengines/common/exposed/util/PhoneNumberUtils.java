package com.latticeengines.common.exposed.util;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.i18n.phonenumbers.NumberParseException;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.Phonenumber.PhoneNumber;

public class PhoneNumberUtils {
    private static final Log log = LogFactory.getLog(PhoneNumberUtils.class);
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
