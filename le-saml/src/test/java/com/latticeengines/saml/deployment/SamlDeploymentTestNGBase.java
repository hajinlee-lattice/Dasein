package com.latticeengines.saml.deployment;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

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
import org.opensaml.xml.security.credential.Credential;
import org.opensaml.xml.signature.Signature;
import org.opensaml.xml.signature.Signer;
import org.opensaml.xml.signature.impl.SignatureBuilder;
import org.opensaml.xml.util.Base64;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.FormHttpMessageConverter;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.security.saml.key.KeyManager;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;
import org.testng.annotations.BeforeClass;

import com.latticeengines.domain.exposed.saml.IdentityProvider;
import com.latticeengines.saml.service.IdentityProviderService;
import com.latticeengines.saml.testframework.SamlTestNGBase;
import com.latticeengines.saml.util.SAMLUtils;
import com.latticeengines.security.entitymanager.GlobalAuthTenantEntityMgr;
import com.latticeengines.testframework.security.GlobalAuthTestBed;

public abstract class SamlDeploymentTestNGBase extends SamlTestNGBase {

    @Autowired
    protected GlobalAuthTestBed globalAuthFunctionalTestBed;

    @Autowired
    private GlobalAuthTenantEntityMgr globalAuthTenantEntityMgr;

    @Autowired
    protected IdentityProviderService identityProviderService;

    @Autowired
    protected ParserPool parserPool;

    @Autowired
    private KeyManager keyManager;

    @Value("${saml.base.address}")
    private String baseUrl;

    protected IdentityProvider identityProvider;

    @BeforeClass(groups = "deployment")
    public void setup() throws InterruptedException {
        setupTenant();

        // Register IdPs
        identityProvider = constructIdp();
        identityProviderService.create(identityProvider);
        // Sleep to let metadata manager pick up the new IdP
        Thread.sleep(10000);
    }

    protected Response getTestSAMLResponse(IdentityProvider idp) {
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
            setResponseIssuedBy(response, idp);
            return response;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void setResponseIssuedBy(Response response, IdentityProvider identityProvider) {
        response.getIssuer().setValue(identityProvider.getEntityId());
        response.getAssertions().get(0).getIssuer().setValue(identityProvider.getEntityId());
    }

    protected void signResponse(Response response) {
        Signature signature = getIdPSignature();
        response.setSignature(signature);
        try {
            Configuration.getMarshallerFactory().getMarshaller(response).marshall(response);
            Signer.signObject(signature);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Signature getIdPSignature() {
        Signature signature = new SignatureBuilder().buildObject();
        Credential credential = keyManager.getCredential("testidp");

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

    protected IdentityProvider constructIdp() {
        IdentityProvider idp = new IdentityProvider();
        idp.setEntityId("http://testidp.lattice-engines.com/" + UUID.randomUUID().toString());
        String metadata = generateMetadata(idp.getEntityId());
        idp.setMetadata(metadata);
        return idp;
    }

    protected String generateMetadata(String entityId) {
        try (InputStream stream = getClass().getResourceAsStream(RESOURCE_BASE + "idp_metadata.xml")) {
            EntityDescriptor descriptor = (EntityDescriptor) SAMLUtils.deserialize(parserPool, stream);
            descriptor.setEntityID(entityId);
            return SAMLUtils.serialize(descriptor);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected ResponseEntity<Void> sendSamlResponse(Response response) {
        signResponse(response);
        String xml = SAMLUtils.serialize(response);
        String encoded = Base64.encodeBytes(xml.getBytes());
        MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
        map.add("SAMLResponse", encoded);
        String url = getSSOEndpointUrl();
        ResponseEntity<Void> httpResponse = getSamlRestTemplate().postForEntity(url, map, Void.class);
        return httpResponse;
    }

}
