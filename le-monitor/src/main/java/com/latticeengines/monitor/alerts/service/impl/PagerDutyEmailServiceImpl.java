package com.latticeengines.monitor.alerts.service.impl;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.message.BasicNameValuePair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.monitor.exposed.alerts.service.PagerDutyService;
import com.latticeengines.monitor.exposed.service.EmailService;
import com.latticeengines.monitor.exposed.service.impl.EmailServiceImpl;

@Component("pagerDutyEmailService")
public class PagerDutyEmailServiceImpl implements PagerDutyService {

    private static final String NA = "na";
    private static final String TEXT_PLAIN = "text/plain";
    private static final String MODULE_NAME = "ConvergedPlatform";
    private static final String PAGERDUTY_EMAIL = "convergedplatform-email@lattice-engines.pagerduty.com";
    private static final String PAGERDUTY_TEST_EMAIL = "convergedplatform-email-test@lattice-engines.pagerduty.com";

    private String pagerDutyEmailAddress;

    @Autowired
    private EmailService emailService;

    public PagerDutyEmailServiceImpl() {
        this.pagerDutyEmailAddress = PAGERDUTY_EMAIL;
    }

    @VisibleForTesting
    public void useTestService() {
        ((EmailServiceImpl) this.emailService).enableEmail();
        this.pagerDutyEmailAddress = PAGERDUTY_TEST_EMAIL;
    }

    @Override
    public String triggerEvent(String description, String clientUrl, String dedupKey, BasicNameValuePair... details)
            throws ClientProtocolException, IOException {

        return this.triggerEvent(description, clientUrl, dedupKey, Arrays.asList(details));
    }

    @Override
    public String triggerEvent(String description, String clientUrl, String dedupKey,
            Iterable<? extends BasicNameValuePair> details) throws ClientProtocolException, IOException {
        String content = getContent(null, clientUrl, dedupKey, details);
        emailService.sendSimpleEmail(StringUtils.defaultString(description, NA), content, TEXT_PLAIN,
                Collections.singleton(pagerDutyEmailAddress));

        return "success";
    }

    private final static String EMAIL_CONTENT = "<Client>%s</Client>%n<ClientUrl>%s</ClientUrl>%n<DeDupKey>%s</DeDupKey>%n<Message>%s</Message>";

    private String getContent(String tenantId, String clientUrl, String dedupKey,
            Iterable<? extends BasicNameValuePair> details) {

        LinkedHashMap<String, String> detailsMap = new LinkedHashMap<>();
        if (details != null) {
            for (Iterator<? extends BasicNameValuePair> iterator = details.iterator(); iterator.hasNext();) {
                BasicNameValuePair detail = iterator.next();
                detailsMap.put(detail.getName(), detail.getValue());
            }
        }
        String detailStr = JsonUtils.serialize(detailsMap);

        String content = String.format(EMAIL_CONTENT, //
                StringUtils.defaultString(MODULE_NAME, NA), //
                StringUtils.defaultString(clientUrl, NA), //
                StringUtils.defaultString(dedupKey, UUID.randomUUID().toString()), //
                detailStr);

        return content;
    }

}
