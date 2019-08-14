package com.latticeengines.monitor.util;

import java.io.IOException;
import java.nio.charset.Charset;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMultipart;
import javax.mail.util.ByteArrayDataSource;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmailTemplateBuilder {

    private static final Logger log = LoggerFactory.getLogger(EmailTemplateBuilder.class);

    private String htmlTemplate;

    public EmailTemplateBuilder(Template template) throws IOException {
        String tmpFile = template.templateFile();
        htmlTemplate = IOUtils.toString(Thread.currentThread().getContextClassLoader().getResourceAsStream(tmpFile),
                Charset.defaultCharset());
    }

    public EmailTemplateBuilder replaceToken(String token, String value) {
        if (value != null) {
            htmlTemplate = htmlTemplate.replace(token, value);
        } else {
            log.error(String.format("Cannot replace token %s with null!", token));
        }
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

    public Multipart buildMultipartWithoutWelcomeHeader() throws MessagingException, IOException {
        Multipart mp = new MimeMultipart();
        MimeBodyPart htmlPart = new MimeBodyPart();
        htmlPart.setContent(htmlTemplate, "text/html");
        mp.addBodyPart(htmlPart);
        MimeBodyPart logoPart = new MimeBodyPart();
        DataSource fds = new ByteArrayDataSource(Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("com/latticeengines/monitor/email_header_with_logo.png"), "image/png");
        logoPart.setDisposition(MimeBodyPart.INLINE);
        logoPart.setDataHandler(new DataHandler(fds));
        logoPart.setHeader("Content-ID", "<banner>");
        mp.addBodyPart(logoPart);
        return mp;
    }

    private static void appendImagesToMultipart(Multipart mp) throws IOException, MessagingException {
        MimeBodyPart logoPart = new MimeBodyPart();
        DataSource fds = new ByteArrayDataSource(Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("com/latticeengines/monitor/email_logo.jpg"), "image/jpeg");
        logoPart.setDisposition(MimeBodyPart.INLINE);
        logoPart.setDataHandler(new DataHandler(fds));
        logoPart.setHeader("Content-ID", "<logo>");
        mp.addBodyPart(logoPart);

        logoPart = new MimeBodyPart();
        fds = new ByteArrayDataSource(Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("com/latticeengines/monitor/email_banner.jpg"), "image/jpeg");
        logoPart.setDisposition(MimeBodyPart.INLINE);
        logoPart.setDataHandler(new DataHandler(fds));
        logoPart.setHeader("Content-ID", "<banner>");
        mp.addBodyPart(logoPart);
    }

    public void addCustomImagesToMultipart(Multipart mp, String imgSrc, String imgType, String cid) throws IOException, MessagingException {
        MimeBodyPart customPart = new MimeBodyPart();
        DataSource fds = new ByteArrayDataSource(Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(imgSrc), imgType);
        customPart.setDisposition(MimeBodyPart.INLINE);
        customPart.setDataHandler(new DataHandler(fds));
        customPart.setHeader("Content-ID", "<" + cid + ">");
        mp.addBodyPart(customPart);
    }

    public enum Template {
        PLS_NEW_EXTERNAL_USER("new_user.html"), //
        PLS_NEW_INTERNAL_USER("new_user.html"), //
        PLS_NEW_PROSPECTING_USER("new_prospecting_user.html"), //
        PLS_EXISTING_EXTERNAL_USER("old_user.html"), //
        PLS_EXISTING_INTERNAL_USER("old_user.html"), //
        PLS_FORGET_PASSWORD("forget_password.html"), //
        PLS_FORGET_PASSWORD_CONFIRMATION("forget_password_confirmation.html"), //
        PD_NEW_EXTERNAL_USER("pd_new_external_user.html"), //
        PD_EXISITING_EXTERNAL_USER("pd_old_external_user.html"), //
        PLS_DEPLOYMENT_STEP_SUCCESS("pls_deployment_step_success.html"), //
        PLS_DEPLOYMENT_STEP_ERROR("pls_deployment_step_error.html"), //
        PLS_ONETIME_SFDC_ACCESS_TOKEN("pls_onetime_sfdc_access_token.html"), SECURITY_GLOBALAUTH_EMAIL_TEMPLATE(
                "security_globalauth_email_template.html"), //
        PLS_JOB_SUCCESS("pls_job_success.html"), //
        PLS_JOB_ERROR("pls_job_error.html"), PLS_JOB_SUCCESS_INTERNAL(
                "pls_job_success_internal.html"), PLS_JOB_ERROR_INTERNAL(
                        "pls_job_error_internal.html"), PLS_INTERNAL_ATTRIBUTE_ENRICH_SUCCESS(
                                "pls_internal_attribute_enrich_success.html"), //
        PLS_INTERNAL_ATTRIBUTE_ENRICH_ERROR(
                "pls_internal_attribute_enrich_error.html"), PLS_INTERNAL_ATTRIBUTE_ENRICH_SUCCESS_INTERNAL(
                        "pls_internal_attribute_enrich_success_internal.html"), PLS_INTERNAL_ATTRIBUTE_ENRICH_ERROR_INTERNAL(
                                "pls_internal_attribute_enrich_error_internal.html"), //

        PLS_EXPORT_SEGMENT_SUCCESS("pls_export_segment_success.html"), //
        PLS_EXPORT_SEGMENT_ERROR("pls_export_segment_error.html"), //
        PLS_EXPORT_SEGMENT_RUNNING("pls_export_segment_running.html"), //

        PLS_EXPORT_ORPHAN_RUNNING("pls_export_orphan_running.html"), //
        PLS_EXPORT_ORPHAN_SUCCESS("pls_export_orphan_success.html"), //

        PLS_CANCEL_ACTION_SUCCESS("pls_cancel_action_success.html"),
        CDL_JOB_SUCCESS("cdl_job_success.html"), CDL_JOB_ERROR("cdl_job_error.html"), //
        TENANT_STATE_NOTICE("poc_state_notice.html"), //
        TENANT_RIGHT_NOTIFY_DAYS("tenant_right_notify_days.html"), //
        TENANT_RIGHT_DELETE("tenant_right_delete.html"), //
        S3_CREDENTIALS("s3_credentials.html"), S3_EMPTY_CREDENTIALS("s3_empty_credentials.html"), //
        CDL_INGESTION_ERROR("cdl_ingestion_error.html"), CDL_INGESTION_SUCCESS(
                "cdl_ingestion_success.html"), CDL_INGESTION_IN_PROCESS("cdl_ingestion_in_progress.html"),
        S3_TEMPLATE_UPDATE("s3_template_update.html"),
        S3_TEMPLATE_CREATE("s3_template_create.html");

        private static final String templateRoot = "com/latticeengines/monitor/";
        private final String templateFile;

        Template(String tmpFile) {
            this.templateFile = templateRoot + tmpFile;
        }

        public String templateFile() {
            return this.templateFile;
        }
    }

}
