package com.latticeengines.monitor.util;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

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

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.util.TemplateUtils;

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

    public EmailTemplateBuilder renderTemplate(Map<String, Object> params) {
        Preconditions.checkNotNull(params, "template parameter map should not be null");
        htmlTemplate = TemplateUtils.renderByMap(htmlTemplate, params);
        return this;
    }

    /**
     * Create a Multipart message object with it's content set to text/html.
     *
     * Don't attach anything to it.
     * @return
     * @throws MessagingException
     */
    public Multipart buildRawMultipart() throws MessagingException {
        Multipart mp = new MimeMultipart();
        MimeBodyPart htmlPart = new MimeBodyPart();
        htmlPart.setContent(htmlTemplate, "text/html");
        mp.addBodyPart(htmlPart);
        return mp;
    }

    /**
     * Create a Multipart message object with content set to text/html and
     * attach 
     * @return
     * @throws MessagingException
     * @throws IOException
     */
    public Multipart buildMultipart() throws MessagingException, IOException {
        Multipart mp = buildRawMultipart();
        appendImagesToMultipart(mp);
        return mp;
    }

    public Multipart buildMultipartWithoutWelcomeHeader() throws MessagingException, IOException {
        Multipart mp = buildRawMultipart();
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

    public void addCustomImagesToMultipart(Multipart mp, String imgSrc, String imgType, String cid)
            throws IOException, MessagingException {
        MimeBodyPart customPart = new MimeBodyPart();
        DataSource fds = new ByteArrayDataSource(
                Thread.currentThread().getContextClassLoader().getResourceAsStream(imgSrc), imgType);
        customPart.setDisposition(MimeBodyPart.INLINE);
        customPart.setDataHandler(new DataHandler(fds));
        customPart.setHeader("Content-ID", "<" + cid + ">");
        mp.addBodyPart(customPart);
    }

    public void addAttachmentToMultipart(Multipart mp, byte[] bytes, String name, String type) throws MessagingException {
        if (bytes != null && bytes.length > 0) {
            MimeBodyPart attachmentPart = new MimeBodyPart();
            attachmentPart.setDataHandler(new DataHandler(new ByteArrayDataSource(bytes, type)));
            attachmentPart.setFileName(name);
            mp.addBodyPart(attachmentPart);
        }
    }

    public enum Template {
        NEW_USER("new_user.html"),//
        EXISTING_USER("old_user.html"),//
        PLS_FORGET_PASSWORD("forget_password.html"), //
        PLS_FORGET_PASSWORD_CONFIRMATION("forget_password_confirmation.html"), //
        PLS_ONETIME_SFDC_ACCESS_TOKEN("pls_onetime_sfdc_access_token.html"), //
        SECURITY_GLOBALAUTH_EMAIL_TEMPLATE("security_globalauth_email_template.html"), //
        PLS_JOB_SUCCESS("pls_job_success.html"), //
        PLS_JOB_ERROR("pls_job_error.html"), //
        PLS_INTERNAL_ATTRIBUTE_ENRICH_SUCCESS("pls_internal_attribute_enrich_success.html"), //
        PLS_INTERNAL_ATTRIBUTE_ENRICH_ERROR("pls_internal_attribute_enrich_error.html"), //

        PLS_EXPORT_SEGMENT_SUCCESS("pls_export_segment_success.html"), //
        PLS_EXPORT_SEGMENT_ERROR("pls_export_segment_error.html"), //
        PLS_EXPORT_SEGMENT_RUNNING("pls_export_segment_running.html"), //

        PLS_EXPORT_ORPHAN_RUNNING("pls_export_orphan_running.html"), //
        PLS_EXPORT_ORPHAN_SUCCESS("pls_export_orphan_success.html"), //

        PLS_ALWAYS_ON_CAMPAIGN_EXPIRATION("pls_always_on_campaign_expiration.html"), //
        PLS_CAMPAIGN_FAILED("pls_campaign_failed.html"), //
        PLS_CAMPAIGN_CANCELED("pls_campaign_canceled.html"), //

        PLS_CANCEL_ACTION_SUCCESS("pls_cancel_action_success.html"), //
        CDL_JOB_SUCCESS("cdl_job_success.html"), //
        CDL_JOB_ERROR("cdl_job_error.html"), //
        TENANT_STATE_NOTICE("poc_state_notice.html"), //
        TENANT_RIGHT_NOTIFY_DAYS("tenant_right_notify_days.html"), //
        TENANT_RIGHT_DELETE("tenant_right_delete.html"), //
        S3_CREDENTIALS("s3_credentials.html"), //
        S3_EMPTY_CREDENTIALS("s3_empty_credentials.html"), //
        CDL_INGESTION_ERROR("cdl_ingestion_error.html"), //
        CDL_INGESTION_SUCCESS("cdl_ingestion_success.html"), //
        CDL_INGESTION_IN_PROCESS("cdl_ingestion_in_progress.html"), //
        S3_TEMPLATE_UPDATE("s3_template_update.html"), //
        S3_TEMPLATE_CREATE("s3_template_create.html"), //
        DNB_INTENT_ALERT("dnb_intent_alert.html"), //
        DCP_UPLOAD_COMPLETED("dcp_upload_completed.html"), //
        DCP_UPLOAD_FAILED("dcp_upload_failed.html"),
        DCP_WELCOME_NEW_USER("dcp_welcome_new_user.html"); //
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
