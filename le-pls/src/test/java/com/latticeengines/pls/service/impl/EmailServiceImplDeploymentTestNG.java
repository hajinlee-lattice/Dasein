package com.latticeengines.pls.service.impl;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.mail.BodyPart;
import javax.mail.Flags;
import javax.mail.Folder;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Store;
import javax.mail.internet.MimeMessage;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ntp.NTPUDPClient;
import org.apache.commons.net.ntp.TimeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.web.client.RestTemplate;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.monitor.EmailSettings;
import com.latticeengines.domain.exposed.pls.RegistrationResult;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.security.exposed.AccessLevel;


/**
 * $ dpltc deploy -a admin,matchapi,microservice,pls -m metadata,lp
 */
public class EmailServiceImplDeploymentTestNG extends PlsDeploymentTestNGBase {

    protected static final Logger log = LoggerFactory.getLogger(EmailServiceImplDeploymentTestNG.class);

    private static final String INTERNAL_USER_EMAIL = "build@lattice-engines.com";
    private static final String EXTERNAL_USER_EMAIL = "build.lattice.engines@gmail.com";
    private static final String EXTERNAL_USER_EMAIL_PASSWORD = "MrB2uild";
    private static final String APP_URL_PATTERN = "href=\"[^\"]*";

    private String testUsername;

    @Value("${security.app.public.url}")
    private String appUrl;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenant();
        deleteUserByRestCall(INTERNAL_USER_EMAIL);
        deleteUserByRestCall(EXTERNAL_USER_EMAIL);
    }

    @AfterClass(groups = { "deployment" })
    public void tearDown() {
        deleteUserByRestCall(INTERNAL_USER_EMAIL);
        deleteUserByRestCall(EXTERNAL_USER_EMAIL);
    }

    @Test(groups = "deployment", enabled = false)
    public void testSendAndReceiveInternalEmail() {
        createNewUserAndSendEmail(INTERNAL_USER_EMAIL);
    }

    @Test(groups = "deployment")
    public void testSendAndReceiveExternalEmail() throws Exception {
        long registrationTime = getTimeWorldClockApi();
        Thread.sleep(1000);
        createNewUserAndSendEmail(EXTERNAL_USER_EMAIL);
        RetryTemplate retry = RetryUtils.getRetryTemplate(10, //
                Collections.singleton(AssertionError.class), null);
        retry.execute(context -> {
            try {
                log.info("(Attempt=" + (context.getRetryCount() + 1) //
                        + ") start verifying receiving emails in Gmail.");
                verifyReceivedEmailInGmail(registrationTime);
                return true;
            } catch (Exception e) {
                throw e;
            }
        });
    }

    private void createNewUserAndSendEmail(String email) {
        UserRegistration userReg = getUserReg(email);
        deleteUserByRestCall(testUsername);

        String json = restTemplate.postForObject(getRestAPIHostPort() + "/pls/users/", userReg, String.class);
        ResponseDocument<RegistrationResult> response = ResponseDocument.generateFromJSON(json,
                RegistrationResult.class);
        assertNotNull(response);
        assertTrue(response.isSuccess(), JsonUtils.serialize(response));
        assertNotNull(response.getResult().getPassword());

        deleteUserByRestCall(testUsername);
    }

    private UserRegistration getUserReg(String email) {
        UserRegistration userReg = new UserRegistration();

        User user = new User();
        user.setEmail(email);
        user.setFirstName("Lattice");
        user.setLastName("Tester");
        user.setPhoneNumber("650-555-5555");
        user.setTitle("Silly Tester");
        user.setAccessLevel(AccessLevel.EXTERNAL_USER.name());

        Credentials creds = new Credentials();
        creds.setUsername(user.getEmail());
        creds.setPassword("WillBeModifiedImmediately");

        user.setUsername(creds.getUsername());

        userReg.setUser(user);
        userReg.setCredentials(creds);

        testUsername = userReg.getCredentials().getUsername();

        return userReg;
    }

    @SuppressWarnings("unused")
    private void verifyReceivedEmailInOutlook() {
    }

    private void verifyReceivedEmailInGmail(long registrationTimestamp) //
            throws MessagingException, IOException {
        String receivingHost = "imap.gmail.com";

        Properties props = System.getProperties();
        props.setProperty("mail.store.protocol", "imaps");
        props.setProperty("mail.imaps.starttls.enable", "true");
        props.setProperty("mail.imaps.port", "993");
        props.setProperty("mail.imaps.ssl.trust", "*");
        Session session = Session.getDefaultInstance(props, null);
        boolean foundTheEmail = false;
        Store store = session.getStore("imaps");
        try {
            String appCode = System.getProperty("GMAIL_APP_PASSWORD");
            String password = StringUtils.isNotBlank(appCode) ? appCode : EXTERNAL_USER_EMAIL_PASSWORD;
            log.info("Using password: [" + appCode + "]");
            store.connect(receivingHost, EXTERNAL_USER_EMAIL, password);
            Folder folder = store.getFolder("INBOX");
            folder.open(Folder.READ_WRITE);
            try {
                Message[] messages = folder.getMessages();
                log.info("There are " + messages.length + " messages in the INBOX.");
                for (Message message : messages) {
                    if (message.getSubject().equalsIgnoreCase(EmailSettings.PLS_NEW_USER_SUBJECT)) {
                        Date receivedDate = message.getReceivedDate();
                        if (receivedDate.getTime() > registrationTimestamp) {
                            MimeMessage m = (MimeMessage) message;
                            String url = getHrefUrlFromMultiPart((Multipart) m.getContent());
                            log.info("Found one new user email received after registration time, with app url " + url);
                            if (appUrl.equalsIgnoreCase(url)) {
                                // delete the message
                                message.setFlag(Flags.Flag.DELETED, true);
                                foundTheEmail = true;
                                break;
                            }
                        } else {
                            log.info("A new user email received at " + receivedDate
                                    + ", before registration time " + new Date(registrationTimestamp));
                        }
                    } else {
                        log.info("Subject: " + message.getSubject());
                    }
                }
            } finally {
                folder.close(true);
            }
        } finally {
            store.close();
        }
        Assert.assertTrue(foundTheEmail, "Did not find the new user email.");
    }

    private static String getHrefUrlFromMultiPart(Multipart multipart) throws MessagingException, IOException {
        for (int j = 0; j < multipart.getCount(); j++) {
            BodyPart bodyPart = multipart.getBodyPart(j);
            if (bodyPart.getDisposition() == null) {
                String content = bodyPart.getContent().toString();
                Pattern pattern = Pattern.compile(APP_URL_PATTERN);
                Matcher matcher = pattern.matcher(content);
                if (matcher.find()) {
                    return matcher.group(0).substring(6);
                }
            }
        }
        return "";
    }

    private long getTimeWorldClockApi() throws IOException {
        long milli;
        try {
            RestTemplate restTemplate = HttpClientUtils.newSSLEnforcedRestTemplate();
            JsonNode jsonNode = restTemplate.getForObject("http://worldclockapi.com/api/json/utc/now", JsonNode.class);
            Assert.assertNotNull(jsonNode);
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm'Z'");
            LocalDateTime localDateTime = LocalDateTime.parse(jsonNode.get("currentDateTime").asText(), formatter);
            milli = localDateTime.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
        } catch (RuntimeException e) {
            NTPUDPClient timeClient = new NTPUDPClient();
            InetAddress inetAddress = InetAddress.getByName("time-a.nist.gov");
            TimeInfo timeInfo = timeClient.getTime(inetAddress);
            milli = timeInfo.getMessage().getTransmitTimeStamp().getTime();
        }
        return milli - TimeUnit.SECONDS.toMillis(30);
    }

}
