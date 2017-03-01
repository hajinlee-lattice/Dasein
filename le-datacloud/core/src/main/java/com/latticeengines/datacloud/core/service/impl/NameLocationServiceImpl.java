package com.latticeengines.datacloud.core.service.impl;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
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

    @Autowired
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

        nameLocation.setName(cleanName);
        nameLocation.setState(cleanState);
        nameLocation.setCountry(cleanCountry);
        nameLocation.setCountryCode(countryCode);
        nameLocation.setCity(cleanCity);

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
