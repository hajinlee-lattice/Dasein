package com.latticeengines.pls.service.impl;

import static com.latticeengines.testframework.exposed.utils.MarketoConnectorHelper.NAME;
import static com.latticeengines.testframework.exposed.utils.MarketoConnectorHelper.REST_CLIENT_ID;
import static com.latticeengines.testframework.exposed.utils.MarketoConnectorHelper.REST_CLIENT_SECRET;
import static com.latticeengines.testframework.exposed.utils.MarketoConnectorHelper.REST_ENDPOINT;
import static com.latticeengines.testframework.exposed.utils.MarketoConnectorHelper.REST_IDENTITY_ENDPOINT;
import static com.latticeengines.testframework.exposed.utils.MarketoConnectorHelper.SOAP_ENCRYPTION_KEY;
import static com.latticeengines.testframework.exposed.utils.MarketoConnectorHelper.SOAP_ENDPOINT;
import static com.latticeengines.testframework.exposed.utils.MarketoConnectorHelper.SOAP_USER_ID;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.MarketoCredential;
import com.latticeengines.domain.exposed.pls.MarketoMatchField;
import com.latticeengines.domain.exposed.pls.MarketoMatchFieldName;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.MarketoCredentialService;
import com.latticeengines.remote.exposed.service.marketo.MarketoRestValidationService;
import com.latticeengines.remote.exposed.service.marketo.MarketoSoapService;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.testframework.exposed.utils.MarketoConnectorHelper;

public class MarketoCredentialServiceImplTestNG extends PlsFunctionalTestNGBase {

    private static final String TENANT1 = "TENANT1";
    private static final String NAME_1 = "TEST MARKETO CREDENTIAL 1";
    private static final String SOAP_ENDPOINT_1 = "UPDATED_SOAP_ENDPOINT";
    private static final String REST_CLIENT_ID_1 = "Updated Rest Client";
    private static final String TEST_DOMAIN_FIELD = "Test Domain";
    private static final String TEST_COMPANY_FIELD = "Test Company";
    private static final String TEST_STATE_FIELD = "Test State";
    private static final String TEST_COUNTRY_FIELD = "Test Country";
    private static final String TEST_DUNS_FIELD = "Test Duns";
    private static final List<String> TEST_FIELD_VALUES = Arrays.asList(TEST_DOMAIN_FIELD, TEST_COMPANY_FIELD,
            TEST_STATE_FIELD, TEST_COUNTRY_FIELD);
    private static final List<MarketoMatchFieldName> MARKETO_MATCH_FIELD_NAMES = Arrays.asList(
            MarketoMatchFieldName.Domain, MarketoMatchFieldName.Company, MarketoMatchFieldName.State,
            MarketoMatchFieldName.Country, MarketoMatchFieldName.DUNS);
    private static final MarketoMatchField MARKETO_MATCH_FIELD_1 = new MarketoMatchField();
    private static final MarketoMatchField MARKETO_MATCH_FIELD_2 = new MarketoMatchField();
    private static final MarketoMatchField MARKETO_MATCH_FIELD_3 = new MarketoMatchField();
    private static final MarketoMatchField MARKETO_MATCH_FIELD_4 = new MarketoMatchField();
    private static final MarketoMatchField MARKETO_MATCH_FIELD_5 = new MarketoMatchField();

    @Autowired
    private MarketoCredentialService marketoCredentialService;

    @Autowired
    private TenantService tenantService;

    private MarketoRestValidationService mockedRestValidationService = Mockito.mock(MarketoRestValidationService.class);
    private MarketoSoapService mockedSoapService = Mockito.mock(MarketoSoapService.class);

    @Value("${pls.marketo.enrichment.webhook.url}")
    private String enrichmentWebhookUrl;

    private void setupTenant(String t) throws Exception {
        Tenant tenant = tenantService.findByTenantId(t);
        if (tenant != null) {
            tenantService.discardTenant(tenant);
        }
        tenant = new Tenant();
        tenant.setId(t);
        tenant.setName(t);
        tenantService.registerTenant(tenant);

        setupSecurityContext(tenant);
    }

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        setupTenant(TENANT1);
        cleanupMarketoCredentialsDB();

        ReflectionTestUtils.setField(marketoCredentialService, "marketoRestValidationService",
                mockedRestValidationService);
        ReflectionTestUtils.setField(marketoCredentialService, "marketoSoapService", mockedSoapService);
        when(mockedRestValidationService.validateMarketoRestCredentials(anyString(), anyString(), anyString(),
                anyString())).thenReturn(true);
        when(mockedSoapService.validateMarketoSoapCredentials(anyString(), anyString(), anyString())).thenReturn(true);
    }

    @Test(groups = "functional")
    public void createMarketoCredential_assertCredentialCreated() throws Exception {
        MarketoCredential marketoCredential = MarketoConnectorHelper.getTestMarketoCredentialConfig();

        marketoCredentialService.createMarketoCredential(marketoCredential);
        List<MarketoCredential> marketoCredentials = marketoCredentialService.findAllMarketoCredentials();
        assertEquals(marketoCredentials.size(), 1);
        MarketoCredential marketoCredential1 = marketoCredentials.get(0);

        assertEquals(marketoCredential1.getName(), NAME);
        assertEquals(marketoCredential1.getSoapEndpoint(), SOAP_ENDPOINT);
        assertEquals(marketoCredential1.getSoapUserId(), SOAP_USER_ID);
        assertEquals(marketoCredential1.getSoapEncryptionKey(), SOAP_ENCRYPTION_KEY);
        assertEquals(marketoCredential1.getRestEndpoint(), REST_ENDPOINT);
        assertEquals(marketoCredential1.getRestIdentityEnpoint(), REST_IDENTITY_ENDPOINT);
        assertEquals(marketoCredential1.getRestClientId(), REST_CLIENT_ID);
        assertEquals(marketoCredential1.getRestClientSecret(), REST_CLIENT_SECRET);
        assertNotNull(marketoCredential1.getLatticeSecretKey());
        assertNotNull(marketoCredential1.getEnrichment());
        assertEquals(marketoCredential1.getEnrichment().getWebhookUrl(), enrichmentWebhookUrl);
        assertEquals(marketoCredential1.getEnrichment().getTenantCredentialGUID(),
                UuidUtils.packUuid(TENANT1, Long.toString(marketoCredential1.getPid())));
        assertEquals(marketoCredential1.getEnrichment().getMarketoMatchFields().size(), 5);
    }

    @Test(groups = "functional", dependsOnMethods = "createMarketoCredential_assertCredentialCreated")
    public void createAnotherMarketoCredential_assertBothAreCreated() throws Exception {
        MarketoCredential marketoCredential = new MarketoCredential();
        marketoCredential.setName(NAME_1);
        marketoCredential.setSoapEndpoint(SOAP_ENDPOINT);
        marketoCredential.setSoapUserId(SOAP_USER_ID);
        marketoCredential.setSoapEncryptionKey(SOAP_ENCRYPTION_KEY);
        marketoCredential.setRestEndpoint(REST_ENDPOINT);
        marketoCredential.setRestIdentityEnpoint(REST_IDENTITY_ENDPOINT);
        marketoCredential.setRestClientId(REST_CLIENT_ID);
        marketoCredential.setRestClientSecret(REST_CLIENT_SECRET);

        marketoCredentialService.createMarketoCredential(marketoCredential);
        List<MarketoCredential> marketoCredentials = marketoCredentialService.findAllMarketoCredentials();

        assertEquals(marketoCredentials.size(), 2);
        List<String> names = Arrays.asList(NAME, NAME_1);
        assertTrue(names.contains(marketoCredentials.get(0).getName()));
        assertTrue(names.contains(marketoCredentials.get(1).getName()));
    }

    @Test(groups = "functional", dependsOnMethods = "createMarketoCredential_assertCredentialCreated")
    public void createAnotherMarketoCredentialWithSameName_assertErrorIsThrown() throws Exception {
        MarketoCredential marketoCredential = new MarketoCredential();
        marketoCredential.setName(NAME);
        marketoCredential.setSoapEndpoint(SOAP_ENDPOINT);
        marketoCredential.setSoapUserId(SOAP_USER_ID);
        marketoCredential.setSoapEncryptionKey(SOAP_ENCRYPTION_KEY);
        marketoCredential.setRestEndpoint(REST_ENDPOINT);
        marketoCredential.setRestIdentityEnpoint(REST_IDENTITY_ENDPOINT);
        marketoCredential.setRestClientId(REST_CLIENT_ID);
        marketoCredential.setRestClientSecret(REST_CLIENT_SECRET);

        try {
            marketoCredentialService.createMarketoCredential(marketoCredential);
            assertFalse(true, "create marketo credential with same name should have thrown exception");
        } catch (Exception e) {
            assertTrue(true, "");
            assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_18119);
        }
    }

    @Test(groups = "functional", dependsOnMethods = "createAnotherMarketoCredential_assertBothAreCreated")
    public void updateOneCredentialToSameNameAsTheOther_assertErrorIsThrown() throws Exception {
        List<MarketoCredential> marketoCredentials = marketoCredentialService.findAllMarketoCredentials();
        assertEquals(marketoCredentialService.findAllMarketoCredentials().size(), 2);

        MarketoCredential marketoCredential = marketoCredentials.get(0);
        marketoCredential.setName(marketoCredentials.get(1).getName());
        try {
            marketoCredentialService.updateMarketoCredentialById(Long.toString(marketoCredential.getPid()),
                    marketoCredential);
            assertFalse(true, "update marketo credential to same name as the other should have thrown exception");
        } catch (Exception e) {
            assertTrue(true, "");
            assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_18119);
        }
    }

    @Test(groups = "functional", dependsOnMethods = "updateOneCredentialToSameNameAsTheOther_assertErrorIsThrown")
    public void deleteCredential_assertCredentialDeleted() {
        List<MarketoCredential> marketoCredentials = marketoCredentialService.findAllMarketoCredentials();
        assertEquals(marketoCredentialService.findAllMarketoCredentials().size(), 2);

        for (MarketoCredential marketoCredential : marketoCredentials) {
            if (marketoCredential.getName().equals(NAME_1)) {
                marketoCredentialService.deleteMarketoCredentialById(Long.toString(marketoCredential.getPid()));
            }
        }

        assertEquals(marketoCredentialService.findAllMarketoCredentials().size(), 1);
    }

    @Test(groups = "functional", dependsOnMethods = "deleteCredential_assertCredentialDeleted")
    public void updateCredential_assertUpdated() {
        MarketoCredential marketoCredential = marketoCredentialService.findAllMarketoCredentials().get(0);
        marketoCredential.setName(NAME_1);
        marketoCredential.setSoapEndpoint(SOAP_ENDPOINT_1);
        marketoCredential.setRestClientId(REST_CLIENT_ID_1);

        marketoCredentialService.updateMarketoCredentialById(Long.toString(marketoCredential.getPid()),
                marketoCredential);

        assertEquals(marketoCredentialService.findAllMarketoCredentials().size(), 1);
        MarketoCredential marketoCredential1 = marketoCredentialService.findAllMarketoCredentials().get(0);
        assertEquals(marketoCredential1.getName(), NAME_1);
        assertEquals(marketoCredential1.getSoapEndpoint(), SOAP_ENDPOINT_1);
        assertEquals(marketoCredential1.getRestClientId(), REST_CLIENT_ID_1);
    }

    @Test(groups = "functional", dependsOnMethods = "updateCredential_assertUpdated")
    public void updateCredentialMatchFields_assertUpdated() {
        MarketoCredential marketoCredential = marketoCredentialService.findAllMarketoCredentials().get(0);
        MARKETO_MATCH_FIELD_1.setMarketoMatchFieldName(MarketoMatchFieldName.Domain);
        MARKETO_MATCH_FIELD_2.setMarketoMatchFieldName(MarketoMatchFieldName.Company);
        MARKETO_MATCH_FIELD_3.setMarketoMatchFieldName(MarketoMatchFieldName.State);
        MARKETO_MATCH_FIELD_4.setMarketoMatchFieldName(MarketoMatchFieldName.Country);
        MARKETO_MATCH_FIELD_5.setMarketoMatchFieldName(MarketoMatchFieldName.DUNS);
        MARKETO_MATCH_FIELD_1.setMarketoFieldName(TEST_DOMAIN_FIELD);
        MARKETO_MATCH_FIELD_2.setMarketoFieldName(TEST_COMPANY_FIELD);
        MARKETO_MATCH_FIELD_3.setMarketoFieldName(TEST_STATE_FIELD);
        MARKETO_MATCH_FIELD_4.setMarketoFieldName(TEST_COUNTRY_FIELD);
        MARKETO_MATCH_FIELD_5.setMarketoFieldName(TEST_DUNS_FIELD);

        marketoCredentialService.updateCredentialMatchFields(Long.toString(marketoCredential.getPid()),
                Arrays.asList(MARKETO_MATCH_FIELD_1, MARKETO_MATCH_FIELD_2, MARKETO_MATCH_FIELD_3,
                        MARKETO_MATCH_FIELD_4, MARKETO_MATCH_FIELD_5));

        MarketoCredential marketoCredential1 = marketoCredentialService.findAllMarketoCredentials().get(0);
        assertEquals(marketoCredential1.getName(), NAME_1);
        assertEquals(marketoCredential1.getSoapEndpoint(), SOAP_ENDPOINT_1);
        assertEquals(marketoCredential1.getRestClientId(), REST_CLIENT_ID_1);
        assertEquals(marketoCredential1.getEnrichment().getMarketoMatchFields().size(), 5);
        for (MarketoMatchField marketoMatchField : marketoCredential1.getEnrichment().getMarketoMatchFields()) {
            MARKETO_MATCH_FIELD_NAMES.contains(marketoMatchField.getMarketoMatchFieldName());
            TEST_FIELD_VALUES.contains(marketoMatchField.getMarketoFieldName());
        }
    }

    @Test(groups = "functional", dependsOnMethods = "updateCredentialMatchFields_assertUpdated")
    public void updateCredentialName_assertAllMatchFieldsPersisted() {
        MarketoCredential marketoCredential = marketoCredentialService.findAllMarketoCredentials().get(0);

        marketoCredential.setName(NAME);

        marketoCredentialService.updateMarketoCredentialById(Long.toString(marketoCredential.getPid()),
                marketoCredential);
        MarketoCredential marketoCredential1 = marketoCredentialService.findAllMarketoCredentials().get(0);
        assertEquals(marketoCredential1.getName(), NAME);
        assertEquals(marketoCredential1.getSoapEndpoint(), SOAP_ENDPOINT_1);
        assertEquals(marketoCredential1.getRestClientId(), REST_CLIENT_ID_1);
        assertEquals(marketoCredential1.getEnrichment().getPid(), marketoCredential.getEnrichment().getPid());
        assertEquals(marketoCredential1.getEnrichment().getMarketoMatchFields().size(), 5);
        for (MarketoMatchField marketoMatchField : marketoCredential1.getEnrichment().getMarketoMatchFields()) {
            MARKETO_MATCH_FIELD_NAMES.contains(marketoMatchField.getMarketoMatchFieldName());
            TEST_FIELD_VALUES.contains(marketoMatchField.getMarketoFieldName());
        }
    }

    @Test(groups = "functional", dependsOnMethods = "updateCredentialName_assertAllMatchFieldsPersisted")
    public void updateCredential_restValidationFailed_assertCorrectErrorReturned() {
        MarketoCredential marketoCredential = marketoCredentialService.findAllMarketoCredentials().get(0);

        when(mockedRestValidationService.validateMarketoRestCredentials(anyString(), anyString(), anyString(),
                anyString())).thenThrow(new LedpException(LedpCode.LEDP_21031)).thenReturn(true);
        try {
            marketoCredentialService.updateMarketoCredentialById(Long.toString(marketoCredential.getPid()),
                    marketoCredential);
            assertFalse(true, "update marketo credential with wrong rest credential should have thrown exception");
        } catch (Exception e) {
            assertTrue(true, "");
            assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_18116);
        }
    }

    @Test(groups = "functional", dependsOnMethods = "updateCredentialName_assertAllMatchFieldsPersisted")
    public void updateCredential_soapValidationFailed_assertCorrectErrorReturned() {
        MarketoCredential marketoCredential = marketoCredentialService.findAllMarketoCredentials().get(0);

        when(mockedSoapService.validateMarketoSoapCredentials(anyString(), anyString(), anyString()))
                .thenThrow(new LedpException(LedpCode.LEDP_21034));
        try {
            marketoCredentialService.updateMarketoCredentialById(Long.toString(marketoCredential.getPid()),
                    marketoCredential);
            assertFalse(true, "update marketo credential with wrong soap credential should have thrown exception");
        } catch (Exception e) {
            assertTrue(true, "");
            assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_18117);
        }
    }

    @Test(groups = "functional", dependsOnMethods = "updateCredentialName_assertAllMatchFieldsPersisted")
    public void findMarketoCredentialById_assertTheRightCredentialIsReturned() {
        MarketoCredential marketoCredential = marketoCredentialService.findAllMarketoCredentials().get(0);

        MarketoCredential marketoCredential1 = marketoCredentialService
                .findMarketoCredentialById(Long.toString(marketoCredential.getPid()));

        assertEquals(marketoCredential1.getPid(), marketoCredential.getPid());
        assertEquals(marketoCredential1.getName(), marketoCredential.getName());
        assertEquals(marketoCredential1.getRestClientId(), marketoCredential.getRestClientId());
        assertEquals(marketoCredential1.getRestClientSecret(), marketoCredential.getRestClientSecret());
        assertEquals(marketoCredential1.getLatticeSecretKey(), marketoCredential.getLatticeSecretKey());
        assertEquals(marketoCredential1.getRestEndpoint(), marketoCredential.getRestEndpoint());
        assertEquals(marketoCredential1.getSoapEncryptionKey(), marketoCredential.getSoapEncryptionKey());
        assertEquals(marketoCredential1.getSoapEndpoint(), marketoCredential.getSoapEndpoint());
        assertEquals(marketoCredential1.getSoapUserId(), marketoCredential.getSoapUserId());
        assertEquals(marketoCredential1.getEnrichment().getPid(), marketoCredential.getEnrichment().getPid());
    }

}
