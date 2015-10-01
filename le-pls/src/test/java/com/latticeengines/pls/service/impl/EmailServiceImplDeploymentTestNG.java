package com.latticeengines.pls.service.impl;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.Date;
import java.util.Properties;
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

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.pls.RegistrationResult;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.security.exposed.AccessLevel;


public class EmailServiceImplDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final String INTERNAL_USER_EMAIL = "build@lattice-engines.com";
    private static final String EXTERNAL_USER_EMAIL = "build.lattice.engines@gmail.com";
    private static final String EXTERNAL_USER_EMAIL_PASSWORD = "MrB2uild";
    private static final String EMAIL_SUBJECT = "Welcome to Lattice Lead Prioritization";
    private static final String APP_URL_PATTERN = "href=\"[^\"]*";

    private String testUsername;

    @Value("${security.pls.app.hostport}")
    private String appUrl;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironment();
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
    public void testSendAndReceiveExternalEmail() throws InterruptedException {
        Date registrationTimestamp = new Date(System.currentTimeMillis());
        createNewUserAndSendEmail(EXTERNAL_USER_EMAIL);
        int numOfRetries = 30;
        boolean verified = false;
        while(numOfRetries-- > 0 && !verified) {
            Thread.sleep(3000L);
            verified = verifyReceivedEmailInGmail(registrationTimestamp);
        }
        Assert.assertTrue(verified, "Should find the new user email in Gmail's INBOX.");
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

    @SuppressWarnings("unused")
    private boolean verifyReceivedEmailInGmail(Date registrationTimestamp) {
        String receivingHost="imap.gmail.com";

        Properties props=System.getProperties();
        props.setProperty("mail.store.protocol", "imaps");
        props.setProperty("mail.imaps.starttls.enable", "true");
        props.setProperty("mail.imaps.port", "993");
        props.setProperty("mail.imaps.ssl.trust", "*");
        Session session=Session.getDefaultInstance(props, null);

        try {
            Store store = session.getStore("imaps");
            String appCode = System.getProperty("GMAIL_APP_PASSWORD");
            String password = StringUtils.isNotEmpty(appCode) ? appCode : EXTERNAL_USER_EMAIL_PASSWORD;
            store.connect(receivingHost, EXTERNAL_USER_EMAIL, password);
            Folder folder = store.getFolder("INBOX");
            folder.open(Folder.READ_WRITE);
            Message messages[] = folder.getMessages();

            for (Message message : messages) {
                if (!message.getSubject().equalsIgnoreCase(EMAIL_SUBJECT)) { continue; }

                Date receivedDate = message.getReceivedDate();

                MimeMessage m = (MimeMessage) message;

                if (message.getReceivedDate().after(registrationTimestamp)) {
                    String url = getHrefUrlFromMultiPart((Multipart) m.getContent());
                    System.out.println("the message url is: " + url);

                    if (appUrl.equalsIgnoreCase(url)) {
                        // delete the message
                        message.setFlag(Flags.Flag.DELETED, true);
                        return true;
                    }

                }
            }

            folder.close(true);
            store.close();

            return false;

        } catch (Exception e) {
            Assert.fail("Failed to verify receiving Gmail.", e);
            return false;
        }
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

}
