package com.latticeengines.datacloud.match.service.impl;

import java.util.UUID;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.datacloud.match.exposed.service.DnBAuthenticationService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBKeyType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchGrade;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.testframework.service.impl.SimpleRetryAnalyzer;
import com.latticeengines.testframework.service.impl.SimpleRetryListener;

@Listeners({ SimpleRetryListener.class })
public class DirectPlusLookupServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DirectPlusLookupServiceImplTestNG.class);

    @Inject
    private DirectPlusRealTimeLookupServiceImpl lookupService;

    @Inject
    private DnBAuthenticationService dnbAuthenticationService;

    private static final int THREAD_NUM = 5;

    @Test(groups = "dnb", dataProvider = "entityInputData", priority = 1, retryAnalyzer = SimpleRetryAnalyzer.class)
    public void testRealTimeEntityLookupService(String name, String city, String state, String country,
            String countryCode, DnBReturnCode expectedDnbCode, String expectedDuns, Integer expectedConfidenceCode,
            DnBMatchGrade expectedMatchGrade) {
        MatchKeyTuple input = new MatchKeyTuple();
        input.setCountry(country);
        input.setCountryCode(countryCode);
        input.setName(name);
        input.setState(state);
        input.setCity(city);
        input.setPhoneNumber("11111");
        DnBMatchContext context = new DnBMatchContext();
        context.setInputNameLocation(input);
        context.setLookupRequestId(UUID.randomUUID().toString());

        DnBMatchContext res = lookupService.realtimeEntityLookup(context);
        Assert.assertNotNull(res.getDuration());
        log.info("InputName={}, DnBReturnCode={}, ConfidenceCode={}, MatchGrade={}, OutOfBusiness={}",
                res.getInputNameLocation().getName(), res.getDnbCode(), res.getConfidenceCode(),
                res.getMatchGrade() != null ? res.getMatchGrade().getRawCode() : null, res.isOutOfBusiness());

        Assert.assertEquals(res.getDnbCode(), expectedDnbCode);
        if (expectedDuns != null) {
            Assert.assertEquals(res.getDuns(), expectedDuns);
        }
        Assert.assertEquals(res.getConfidenceCode(), expectedConfidenceCode);
        Assert.assertEquals(res.getMatchGrade(), expectedMatchGrade);
        Assert.assertNotNull(res.getDuration());
        log.info("Match duration: {}", res.getDuration());
        if (res.getMatchGrade() != null) {
            Assert.assertNotNull(res.getMatchGrade().getRawCode());
        }

    }

    @Test(groups = "dnb", retryAnalyzer = SimpleRetryAnalyzer.class)
    public void loadTestRealTimeLookupService() {
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_NUM);
        CompletionService<DnBMatchContext> cs = new ExecutorCompletionService<>(executorService);

        for (int i = 0; i < THREAD_NUM; i++) {
            cs.submit(() -> {
                MatchKeyTuple input = new MatchKeyTuple();
                input.setCountry("USA");
                input.setCountryCode("US");
                input.setName("Google");
                input.setState("CA");
                DnBMatchContext context = new DnBMatchContext();
                context.setInputNameLocation(input);
                return lookupService.realtimeEntityLookup(context);
            });
            log.info("Submit {}th request", i);
        }

        for (int i = 0; i < THREAD_NUM; i++) {
            try {
                DnBMatchContext result = cs.take().get();
                Assert.assertEquals(result.getDnbCode(), DnBReturnCode.OK);
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            } catch (ExecutionException e) {
                log.error(e.getMessage(), e);
                Assert.fail();
            }
        }
    }

    // Test is disabled due to email lookup is not actively used in application
    @Test(groups = "dnb", dataProvider = "emailInputData", enabled = false, priority = 2, retryAnalyzer = SimpleRetryAnalyzer.class)
    public void testRealTimeEmailLookupService(String email, DnBReturnCode expectedDnbCode, String expectedDuns) {
        DnBMatchContext context = new DnBMatchContext();
        context.setInputEmail(email);
        DnBMatchContext res = lookupService.realtimeEmailLookup(context);
        Assert.assertEquals(res.getDnbCode(), expectedDnbCode);
        Assert.assertEquals(res.getDuns(), expectedDuns);
        Assert.assertNotNull(res.getDuration());
        log.info(String.format("Match duration: %d", res.getDuration()));
    }

    @Test(groups = "dnb", dataProvider = "entityInputDataTestMatchedNameLocation", enabled = true, priority = 3, retryAnalyzer = SimpleRetryAnalyzer.class)
    public void testRealTimeEntityLookupMatchedNameLocation(String inputName, String inputCountry,
            String inputCountryCode, DnBReturnCode dnbCode, String duns, Integer ConfidenceCode,
            DnBMatchGrade matchGrade, String matchedName, String matchedStreet, String matchedCity, String matchedState,
            String matchedCountryCode, String matchedZipCode, String matchedPhoneNumber) {
        MatchKeyTuple input = new MatchKeyTuple();
        input.setCountry(inputCountry);
        input.setCountryCode(inputCountryCode);
        input.setName(inputName);
        DnBMatchContext context = new DnBMatchContext();
        context.setInputNameLocation(input);
        context.setLookupRequestId(UUID.randomUUID().toString());

        DnBMatchContext res = lookupService.realtimeEntityLookup(context);
        Assert.assertNotNull(res.getDuration());
        log.info(
                "MatchGrade = {}, Name = {}, Street = {}, City = {}, State = {}, CountryCode = {}, ZipCode = {}, PhoneNumber = {}, OutOfBusiness = {}",
                res.getMatchGrade() != null ? res.getMatchGrade().getRawCode() : null,
                res.getMatchedNameLocation().getName(), res.getMatchedNameLocation().getStreet(),
                res.getMatchedNameLocation().getCity(), res.getMatchedNameLocation().getState(),
                res.getMatchedNameLocation().getCountryCode(), res.getMatchedNameLocation().getZipcode(),
                res.getMatchedNameLocation().getPhoneNumber(), res.isOutOfBusiness());

        Assert.assertEquals(res.getDnbCode(), dnbCode);
        if (duns != null) {
            Assert.assertEquals(res.getDuns(), duns);
        }
        Assert.assertEquals(res.getConfidenceCode(), ConfidenceCode);
        Assert.assertEquals(res.getMatchGrade(), matchGrade);
        Assert.assertNotNull(res.getDuration());
        Assert.assertEquals(res.getMatchedNameLocation().getName(), matchedName);
        Assert.assertEquals(res.getMatchedNameLocation().getStreet(), matchedStreet);
        Assert.assertEquals(res.getMatchedNameLocation().getCity(), matchedCity);
        Assert.assertEquals(res.getMatchedNameLocation().getState(), matchedState);
        Assert.assertEquals(res.getMatchedNameLocation().getCountryCode(), matchedCountryCode);
        Assert.assertEquals(res.getMatchedNameLocation().getZipcode(), matchedZipCode);
        Assert.assertEquals(res.getMatchedNameLocation().getPhoneNumber(), matchedPhoneNumber);
    }

    /**
     * Notice that DnB is changing error response schema periodically without
     * notification. Should update the error response schema in the test up to date
     * if there is any change detected.
     */
    @Test(groups = "functional", retryAnalyzer = SimpleRetryAnalyzer.class)
    public void testParseDnBHttpError() {
        Assert.assertEquals(lookupService.parseDnBHttpError(new HttpClientErrorException(HttpStatus.UNAUTHORIZED,
                HttpStatus.UNAUTHORIZED.name(),
                ("{\"transactionDetail\":{\"transactionID\":\"rrt-0623a0192b166e525-c-ea-16523-119576738-21\",\"transactionTimestamp\":\"2020-04-11T17:18:37.582Z\",\"inLanguage\":\"en-US\",\"serviceVersion\":null},\"error\":{\"errorMessage\":\"You are not currently authorised to access this product. Please contact your D&B account representative\",\"errorCode\":\"00004\"}}")
                        .getBytes(),
                null)), DnBReturnCode.UNAUTHORIZED);
    }

    @Test(groups = "dnb", priority = 4, retryAnalyzer = SimpleRetryAnalyzer.class)
    public void testInvalidToken() {
        // Set token to be invalid
        dnbAuthenticationService.refreshToken(DnBKeyType.DPLUS, "abc");
        // Wait for local cache to be refreshed
        SleepUtils.sleep(5000L);
        // Expected the service to refresh token and make a successful call via
        // retry
        MatchKeyTuple tuple = new MatchKeyTuple();
        tuple.setCountry("USA");
        tuple.setCountryCode("US");
        tuple.setName("Google");
        DnBMatchContext context = new DnBMatchContext();
        context.setInputNameLocation(tuple);
        context.setLookupRequestId(UUID.randomUUID().toString());

        DnBMatchContext res = lookupService.realtimeEntityLookup(context);
        Assert.assertEquals(res.getDnbCode(), DnBReturnCode.OK);
    }

    @DataProvider(name = "entityInputData")
    public static Object[][] getEntityInputData() {
        return new Object[][]{
                { "BENCHMARK BLINDS", "GILBERT", "ARIZONA", "USA", "US", DnBReturnCode.OK, "038796548", 8,
                        new DnBMatchGrade("AZZAAZZZFAB") },
                { "DÉSIRÉE DAUDE", null, null, "GERMANY", "DE", DnBReturnCode.OK, null, 4,
                        new DnBMatchGrade("BZZZZZZZZZZ") },
                {"ABCDEFG", "NEW YORK", "WASHINTON", "USA", "US", DnBReturnCode.UNMATCH, null, null, null},
                { "GORMAN MANUFACTURING", null, null, "USA", "US", DnBReturnCode.OK, "804735132", 6,
                        new DnBMatchGrade("AZZZZZZZFZZ") },
                { "GOOGLE", null, "CA", "USA", "US", DnBReturnCode.OK, "060902413", 6,
                        new DnBMatchGrade("AZZZAZZZFFZ") },
                { "GOOGLE GERMANY", "HAMBURG", null, "GERMANY", "DE", DnBReturnCode.OK, "330465266", 7,
                        new DnBMatchGrade("AZZAZZZZZFZ") },
                { "GORMAN MFG CO INC", "SACRAMENTO", "CA", "USA", "US", DnBReturnCode.OK, "009175688", 7,
                        new DnBMatchGrade("AZZAAZZZFFZ") },
        };
    }

    @DataProvider(name = "emailInputData")
    public static Object[][] getEmailInputData() {
        return new Object[][] { { "CRISTIANA_MAURICIO@DEACONESS.COM", DnBReturnCode.UNMATCH, null },
                { "JREMLEY@GOOGLE.COM", DnBReturnCode.OK, "060902413" } };
    }

    @DataProvider(name = "entityInputDataTestMatchedNameLocation")
    public static Object[][] getEntityInputDataTestMatchedNameLocation() {
        return new Object[][] {
                { "GOOGLE", "USA", "US", DnBReturnCode.OK, "060902413", 6, new DnBMatchGrade("AZZZZZZZFZZ"),
                        "GOOGLE LLC", "1600 AMPHITHEATRE PKWY", "MOUNTAIN VIEW", "CA", "US", "94043", "6502530000" }, };
    }
}
