package com.latticeengines.datacloud.core.service.impl;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.LocationUtils;
import com.latticeengines.common.exposed.util.NameStringStandardizationUtils;
import com.latticeengines.common.exposed.util.PhoneNumberUtils;
import com.latticeengines.common.exposed.util.StringStandardizationUtils;
import com.latticeengines.datacloud.core.service.CountryCodeService;
import com.latticeengines.datacloud.core.service.NameLocationService;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;

@Component("nameLocationService")
public class NameLocationServiceImpl implements NameLocationService {

    @Inject
    private CountryCodeService countryCodeService;

    @Override
    public void normalize(NameLocation nameLocation) {

        String cleanName = NameStringStandardizationUtils.getStandardString(nameLocation.getName());
        String cleanCountry = countryCodeService.getStandardCountry(nameLocation.getCountry());
        String countryCode = StringUtils.isNotEmpty(nameLocation.getCountryCode()) ? nameLocation.getCountryCode()
                : countryCodeService.getCountryCode(nameLocation.getCountry());
        String cleanState = LocationUtils.getStandardState(cleanCountry, nameLocation.getState());
        String cleanCity = NameStringStandardizationUtils.getStandardString(nameLocation.getCity());
        String cleanPhoneNumber = PhoneNumberUtils.getStandardPhoneNumber(nameLocation.getPhoneNumber(), countryCode);
        String cleanZipCode = StringStandardizationUtils.getStandardString(nameLocation.getZipcode());
        String cleanStreet = StringStandardizationUtils.getStandardString(nameLocation.getStreet());
        String cleanStreet2 = StringStandardizationUtils.getStandardString(nameLocation.getStreet2());


        nameLocation.setName(cleanName);
        nameLocation.setState(cleanState);
        nameLocation.setCountry(cleanCountry);
        nameLocation.setCountryCode(countryCode);
        nameLocation.setCity(cleanCity);
        nameLocation.setStreet(cleanStreet);
        nameLocation.setStreet2(cleanStreet2);

        nameLocation.setZipcode(cleanZipCode);
        nameLocation.setPhoneNumber(cleanPhoneNumber);
    }

    @Override
    public void setDefaultCountry(NameLocation nameLocation) {
        if (StringUtils.isEmpty(nameLocation.getCountry())) {
            nameLocation.setCountry(LocationUtils.USA);
            nameLocation.setCountryCode(countryCodeService.getCountryCode(LocationUtils.USA));
            nameLocation.setState(LocationUtils.getStandardState(LocationUtils.USA, nameLocation.getState()));
        }
    }

}
