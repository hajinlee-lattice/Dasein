package com.latticeengines.monitor.exposed.service.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import javax.mail.Multipart;

import org.apache.commons.io.IOUtils;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.security.Tenant;

public class EmailServiceImplUnitTestNG {
    private static final Logger log = LoggerFactory.getLogger(EmailServiceImplUnitTestNG.class);

    private static final String INTENT_EMAIL_SUBJECT = "Intent Alert (09/02/2020 - 09/08/2020)";
    private static final List<String> RECIPIENTS = Arrays.asList("j.doe@dnb.com", "a.doe@dnb.com");

    @Test(groups = "unit")
    private void testSendDnbIntentAlertEmail() {
        EmailServiceImpl svc = Mockito.mock(EmailServiceImpl.class);
        doCallRealMethod().when(svc).sendDnbIntentAlertEmail(any(), anyCollection(), anyString(), anyMap());

        AtomicReference<Multipart> emailBody = new AtomicReference<>(null);
        doAnswer(invocation -> {
            emailBody.set(invocation.getArgument(1));
            // verify email subject and recipients
            Assert.assertEquals(invocation.getArgument(0), INTENT_EMAIL_SUBJECT);
            Assert.assertEquals(invocation.getArgument(2), RECIPIENTS);
            return null;
        }).when(svc).sendMultiPartEmail(anyString(), any(), anyCollection());

        Map<String, Object> params = getDnBIntentAlertEmailParams();
        svc.sendDnbIntentAlertEmail(testTenant(), RECIPIENTS, INTENT_EMAIL_SUBJECT, params);

        Mockito.verify(svc, Mockito.times(1)).sendDnbIntentAlertEmail(any(), anyCollection(), anyString(), anyMap());
        // TODO maybe also verify rendered email body
        Assert.assertNotNull(emailBody.get());
    }

    private Tenant testTenant() {
        String tenantId = EmailServiceImplUnitTestNG.class.getSimpleName() + "_" + UUID.randomUUID().toString();
        return new Tenant(tenantId);
    }

    private Map<String, Object> getDnBIntentAlertEmailParams() {
        try {
            InputStream is = Thread.currentThread().getContextClassLoader()
                    .getResourceAsStream("com/latticeengines/monitor/dnb_intent_alert_data.json");
            Preconditions.checkNotNull(is);
            String data = IOUtils.toString(is, Charset.defaultCharset());
            Map<?, ?> obj = JsonUtils.deserialize(data, Map.class);
            return JsonUtils.convertMap(obj, String.class, Object.class);
        } catch (Exception e) {
            log.error("failed to parse");
            throw new RuntimeException(e);
        }
    }
}
