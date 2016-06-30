package com.latticeengines.saml.functionalframework;

import java.io.IOException;
import java.io.InputStream;

import org.joda.time.DateTime;
import org.opensaml.saml2.core.Assertion;
import org.opensaml.saml2.core.Response;
import org.opensaml.saml2.core.SubjectConfirmation;
import org.opensaml.saml2.core.SubjectConfirmationData;
import org.opensaml.saml2.metadata.EntityDescriptor;
import org.opensaml.xml.Configuration;
import org.opensaml.xml.parse.ParserPool;
import org.opensaml.xml.security.SecurityException;
import org.opensaml.xml.security.SecurityHelper;
import org.opensaml.xml.security.x509.X509Credential;
import org.opensaml.xml.signature.Signature;
import org.opensaml.xml.signature.Signer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.converter.FormHttpMessageConverter;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.security.saml.key.KeyManager;
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

    @Autowired
    private KeyManager keyManager;

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
            assertion.getAuthnStatements().get(0).setAuthnInstant(DateTime.now());

            assertion.getConditions().getAudienceRestrictions().get(0).getAudiences().get(0).setAudienceURI( //
                    SAMLUtils.getEntityIdFromTenantId(globalAuthFunctionalTestBed.getMainTestTenant().getId()));
            assertion.getSubject().getNameID()
                    .setValue(globalAuthFunctionalTestBed.getCurrentUser().getResult().getUser().getEmailAddress());
            signResponse(response);
            return response;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void signResponse(Response response) {
        Signature signature = getTestingIdPSignature();
        response.setSignature(signature);
        try {
            Configuration.getMarshallerFactory().getMarshaller(response).marshall(response);
            Signer.signObject(signature);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Signature getTestingIdPSignature() {
        Signature signature = (Signature) Configuration.getBuilderFactory().getBuilder(Signature.DEFAULT_ELEMENT_NAME)
                .buildObject(Signature.DEFAULT_ELEMENT_NAME);
        X509Credential credential = (X509Credential) keyManager.getCredential("testidp");

        try {
            signature.setSigningCredential(credential);
            SecurityHelper.prepareSignatureParams(signature, credential,
                    Configuration.getGlobalSecurityConfiguration(), null);
        } catch (SecurityException e) {
            throw new RuntimeException("Error attempting to build signature", e);
        }

        return signature;
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
