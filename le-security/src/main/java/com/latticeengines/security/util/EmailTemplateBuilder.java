package com.latticeengines.security.util;

import java.io.IOException;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMultipart;
import javax.mail.util.ByteArrayDataSource;

import org.apache.commons.io.IOUtils;

public class EmailTemplateBuilder {

    private String htmlTemplate;

    public EmailTemplateBuilder(Template template) throws IOException {
        String tmpFile = template.templateFile();
        htmlTemplate = IOUtils.toString(Thread.currentThread().getContextClassLoader().getResourceAsStream(tmpFile));
    }

    public EmailTemplateBuilder replaceToken(String token, String value) {
        htmlTemplate = htmlTemplate.replace(token, value);
        return this;
    }

    public Multipart buildMultipart() throws MessagingException, IOException {
        Multipart mp = new MimeMultipart();
        MimeBodyPart htmlPart = new MimeBodyPart();
        htmlPart.setContent(htmlTemplate, "text/html");
        mp.addBodyPart(htmlPart);
        appendImagesToMultipart(mp);
        return mp;
    }

    private static void appendImagesToMultipart(Multipart mp) throws IOException, MessagingException {
        MimeBodyPart logoPart = new MimeBodyPart();
        DataSource fds = new ByteArrayDataSource(Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("com/latticeengines/security/email_logo.jpg"), "image/jpeg");
        logoPart.setDisposition(MimeBodyPart.INLINE);
        logoPart.setDataHandler(new DataHandler(fds));
        logoPart.setHeader("Content-ID", "<logo>");
        mp.addBodyPart(logoPart);

        logoPart = new MimeBodyPart();
        fds = new ByteArrayDataSource(Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("com/latticeengines/security/email_banner.jpg"), "image/jpeg");
        logoPart.setDisposition(MimeBodyPart.INLINE);
        logoPart.setDataHandler(new DataHandler(fds));
        logoPart.setHeader("Content-ID", "<banner>");
        mp.addBodyPart(logoPart);
    }

    public enum Template {
        PLS_NEW_EXTERNAL_USER("new_user.html"), //
        PLS_NEW_INTERNAL_USER("new_user.html"), //
        PLS_EXISTING_EXTERNAL_USER("old_user.html"), //
        PLS_EXISTING_INTERNAL_USER("old_user.html"), //
        PLS_FORGET_PASSWORD("forget_password.html"), //
        PD_NEW_EXTERNAL_USER("pd_new_external_user.html"), //
        PD_NEW_INTERNAL_USER("pd_new_internal_user.html"), //
        PD_EXISTING_EXTERNAL_USER("pd_old_external_user.html"), //
        PD_EXISTING_INTERNAL_USER("pd_old_internal_user.html"), //
        PLS_DEPLOYMENT_STEP_SUCCESS("pls_deployment_step_success.html"), //
        PLS_DEPLOYMENT_STEP_ERROR("pls_deployment_step_error.html"), //
        PLS_ONETIME_SFDC_ACCESS_TOKEN("pls_onetime_sfdc_access_token.html"), //
        SECURITY_GLOBALAUTH_EMAIL_TEMPLATE("security_globalauth_email_template.html");

        private final static String templateRoot = "com/latticeengines/security/";
        private final String templateFile;

        Template(String tmpFile) {
            this.templateFile = templateRoot + tmpFile;
        }

        public String templateFile() {
            return this.templateFile;
        }
    }

}
