package com.latticeengines.datacloud.match.service.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.LocationUtils;
import com.latticeengines.datacloud.match.service.CountryCodeService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;

@Component
public class CountryCodeServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {
    private static final Log log = LogFactory.getLog(CountryCodeServiceImplTestNG.class);
    private static final String FILENAME = "CountryCode.csv";

    @Autowired
    private CountryCodeService countryCodeService;

    @Test(groups = "functional")
    public void testSingleMapping() {
        String standardizedCountry = LocationUtils.getStandardCountry("United States");
        Assert.assertEquals(standardizedCountry, "USA");
        String countryCode = countryCodeService.getCountryCode(standardizedCountry);
        Assert.assertEquals(countryCode, "US");
    }

    @Test(groups = "functional")
    public void bulkTest() {
        InputStream fileStream = ClassLoader.getSystemResourceAsStream("matchinput/" + FILENAME);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fileStream));
        boolean res = true;
        String countryName;
        try {
            while ((countryName = reader.readLine()) != null) {
                String standardizedCountry = LocationUtils.getStandardCountry(countryName);
                String countryCode = countryCodeService.getCountryCode(standardizedCountry);
                if (countryCode == null) {
                    res = false;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        Assert.assertTrue(res);
    }
}
