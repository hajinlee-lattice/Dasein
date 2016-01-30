package com.latticeengines.propdata.match.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.manage.MatchInput;
import com.latticeengines.domain.exposed.propdata.manage.MatchKey;
import com.latticeengines.domain.exposed.propdata.manage.MatchStatus;
import com.latticeengines.domain.exposed.propdata.manage.OutputRecord;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.propdata.match.testframework.PropDataMatchFunctionalTestNGBase;


@Component
public class RealTimeMatchServiceCacheImplTestNG extends PropDataMatchFunctionalTestNGBase {

    @Autowired
    private RealTimeMatchServiceCacheImpl matchService;

    @Test(groups = "functional")
    public void testInputValidationForRealtime() {
        MatchInput input = new MatchInput();
        boolean failed = false;
        try {
            matchService.validateMatchInput(input);
        } catch (Exception e) {
            failed = true;
        }
        Assert.assertTrue(failed, "Should failed on missing tenant.");

        failed = false;
        try {
            matchService.validateMatchInput(input);
        } catch (Exception e) {
            failed = true;
        }
        Assert.assertTrue(failed, "Should failed on missing match type.");

        input.setMatchType(MatchInput.MatchType.RealTime);

        failed = false;
        input.setTenant(new Tenant("PD_Test"));
        try {
            matchService.validateMatchInput(input);
        } catch (Exception e) {
            failed = true;
        }
        Assert.assertTrue(failed, "Should failed on empty key list.");


        failed = false;
        input.setKeys(Arrays.asList(MatchKey.Domain, MatchKey.Domain));
        try {
            matchService.validateMatchInput(input);
        } catch (Exception e) {
            failed = true;
        }
        Assert.assertTrue(failed, "Should failed on duplicated keys.");

        failed = false;
        input.setKeys(Arrays.asList(MatchKey.Domain, MatchKey.Name, MatchKey.City, MatchKey.State, MatchKey.Country));
        try {
            matchService.validateMatchInput(input);
        } catch (Exception e) {
            failed = true;
        }
        Assert.assertTrue(failed, "Should failed on empty data.");

        failed = false;
        input.setData(generateMockData(2000));
        try {
            matchService.validateMatchInput(input);
        } catch (Exception e) {
            failed = true;
        }
        Assert.assertTrue(failed, "Should failed on too many data.");

        input.setData(generateMockData(100));
        matchService.validateMatchInput(input);
    }

    @Test(groups = "functional")
    public void testPrepareOutput() {
        MatchInput input = new MatchInput();
        input.setTenant(new Tenant("PD_Test"));
        input.setKeys(Arrays.asList(MatchKey.Domain, MatchKey.Name, MatchKey.City, MatchKey.State, MatchKey.Country));

        List<List<Object>> mockData = generateMockData(100);
        input.setData(mockData);

        Set<String> uniqueDomains = new HashSet<>();
        for (List<Object> row: mockData) {
            String domain = (String) row.get(0);
            Assert.assertTrue(domain.contains("abc@"));
            uniqueDomains.add(domain);
        }

        MatchContext context = matchService.prepare(input, true);
        Assert.assertEquals(context.getStatus(), MatchStatus.NEW);
        Assert.assertEquals(context.getDomains().size(), uniqueDomains.size());

        for (OutputRecord record: context.getOutput().getResult()) {
            Assert.assertFalse(record.isMatched());
            Assert.assertFalse(record.getMatchedDomain().contains("abc@"));
        }
    }

    private List<List<Object>> generateMockData(int rows) {
        List<List<Object>> data = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
            String domain = "abc@" + randomString(6) + ".com";
            String name = randomString(20);
            String city = randomString(20);
            String state = randomString(10);
            String country = "USA";
            List<Object> row = Arrays.asList((Object) domain, name, city, state, country);
            data.add(row);
        }
        return data;
    }

    private String randomString(int length)
    {
        Random random = new Random();
        String characters = "abcdefghijklmnopqrstuvwxyz0123456789";
        char[] text = new char[length];
        for (int i = 0; i < length; i++)
        {
            text[i] = characters.charAt(random.nextInt(characters.length()));
        }
        return new String(text);
    }

}
