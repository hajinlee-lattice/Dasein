package com.latticeengines.saml.functionalframework;

import java.io.IOException;
import java.io.InputStream;

import org.joda.time.DateTime;
import org.opensaml.saml2.core.Assertion;
import org.opensaml.saml2.core.Response;
import org.opensaml.saml2.core.SubjectConfirmation;
import org.opensaml.saml2.core.SubjectConfirmationData;
import org.opensaml.saml2.metadata.EntityDescriptor;
import org.opensaml.xml.parse.ParserPool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.converter.FormHttpMessageConverter;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.web.client.RestTemplate;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;

import com.latticeengines.domain.exposed.saml.IdentityProvider;
import com.latticeengines.saml.entitymgr.IdentityProviderEntityMgr;
import com.latticeengines.saml.util.SAMLUtils;
import com.latticeengines.security.entitymanager.GlobalAuthTenantEntityMgr;
import com.latticeengines.testframework.security.GlobalAuthTestBed;
import com.latticeengines.testframework.security.impl.GlobalAuthCleanupTestListener;

@Listeners({ GlobalAuthCleanupTestListener.class })
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-saml-context.xml" })
public abstract class SamlDeploymentTestNGBase extends AbstractTestNGSpringContextTests {
    public static final String RESOURCE_BASE = "/com/latticeengines/saml/";

    @Autowired
    protected GlobalAuthTestBed globalAuthFunctionalTestBed;

    @Autowired
    private GlobalAuthTenantEntityMgr globalAuthTenantEntityMgr;

    @Autowired
    private IdentityProviderEntityMgr identityProviderEntityMgr;

    @Autowired
    protected ParserPool parserPool;

    @Value("${saml.base.address}")
    private String baseUrl;

    private IdentityProvider identityProvider;

    @BeforeClass(groups = "deployment")
    public void setup() throws InterruptedException {
        // Create test tenant
        globalAuthFunctionalTestBed.bootstrap(1);

        // Create test user
        globalAuthFunctionalTestBed.switchToInternalAdmin();

        // Register IdPs
        identityProvider = constructIdp();
        identityProviderEntityMgr.create(identityProvider);
        // Sleep to let metadata manager pick up the new IdP
        Thread.sleep(10000);
    }

    @AfterClass(groups = "deployment")
    public void teardown() {
        identityProviderEntityMgr.delete(identityProvider);
    }

    protected Response getTestSAMLResponse() {
        try (InputStream stream = getClass().getResourceAsStream(RESOURCE_BASE + "test_response.xml")) {
            Response response = (Response) SAMLUtils.deserialize(parserPool, stream);
            response.setIssueInstant(DateTime.now());
            response.setDestination(getSSOEndpointUrl());
            Assertion assertion = response.getAssertions().get(0);
            assertion.setIssueInstant(DateTime.now());
            assertion.getConditions().setNotOnOrAfter(DateTime.now().plus(60000));
            SubjectConfirmation sc = assertion.getSubject().getSubjectConfirmations().get(0);
            SubjectConfirmationData scd = sc.getSubjectConfirmationData();
            scd.setNotOnOrAfter(DateTime.now().plus(60000));
            scd.setRecipient(getSSOEndpointUrl());
            assertion.getConditions().getAudienceRestrictions().get(0).getAudiences().get(0).setAudienceURI( //
                    SAMLUtils.getEntityIdFromTenantId(globalAuthFunctionalTestBed.getMainTestTenant().getId()));
            assertion.getSubject().getNameID()
                    .setValue(globalAuthFunctionalTestBed.getCurrentUser().getResult().getUser().getEmailAddress());
            return response;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected String getSSOEndpointUrl() {
        return baseUrl + "/SSO/alias/" + globalAuthFunctionalTestBed.getMainTestTenant().getId();
    }

    protected RestTemplate getSamlRestTemplate() {
        RestTemplate restTemplate = new RestTemplate();
        HttpMessageConverter formHttpMessageConverter = new FormHttpMessageConverter();
        HttpMessageConverter stringHttpMessageConverter = new StringHttpMessageConverter();
        restTemplate.getMessageConverters().add(formHttpMessageConverter);
        restTemplate.getMessageConverters().add(stringHttpMessageConverter);
        return restTemplate;
    }

    private IdentityProvider constructIdp() {
        IdentityProvider idp = new IdentityProvider();
        String tenantId = globalAuthFunctionalTestBed.getMainTestTenant().getId();
        idp.setEntityId("http://www.okta.com/exk6g63nlfTEMP8VC0h7");
        idp.setGlobalAuthTenant(globalAuthTenantEntityMgr.findByTenantId(tenantId));
        String metadata = generateMetadata();
        idp.setMetadata(metadata);
        return idp;
    }

    private String generateMetadata() {
        try (InputStream stream = getClass().getResourceAsStream(RESOURCE_BASE + "idp_metadata.xml")) {
            EntityDescriptor descriptor = (EntityDescriptor) SAMLUtils.deserialize(parserPool, stream);
            return SAMLUtils.serialize(descriptor);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
