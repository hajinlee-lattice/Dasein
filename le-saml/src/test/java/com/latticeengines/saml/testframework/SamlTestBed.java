package com.latticeengines.saml.testframework;

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

import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.domain.exposed.saml.IdentityProvider;
import com.latticeengines.saml.util.SAMLUtils;
import com.latticeengines.security.exposed.util.MultiTenantContext;
import com.latticeengines.testframework.exposed.service.GlobalAuthTestBed;

public abstract class SamlTestBed {
    public static final String RESOURCE_BASE = "/com/latticeengines/saml/";

    @Autowired
    private ParserPool parserPool;

    @Autowired
    private KeyManager keyManager;

    @Value("${saml.base.address}")
    protected String baseUrl;

    public abstract GlobalAuthTestBed getGlobalAuthTestBed();

    public abstract void registerIdentityProvider(IdentityProvider identityProvider);

    public void setupTenant() {
        // Create test tenant
        getGlobalAuthTestBed().bootstrap(2);
        MultiTenantContext.setTenant(getGlobalAuthTestBed().getMainTestTenant());

        // Create test user
        getGlobalAuthTestBed().switchToInternalAdmin();
    }

    public Response getTestSAMLResponse(IdentityProvider idp) {
        try (InputStream stream = getClass().getResourceAsStream(RESOURCE_BASE + "test_response.xml")) {
            Response response = (Response) SAMLUtils.deserialize(parserPool, stream);
            response.setIssueInstant(DateTime.now());
            response.setDestination(getSSOEndpointUrl());
            Assertion assertion = response.getAssertions().get(0);
            assertion.setIssueInstant(DateTime.now());
            assertion.getConditions().setNotOnOrAfter(DateTime.now().plus(60000));
            assertion.getConditions().setNotBefore(DateTime.now());
            SubjectConfirmation sc = assertion.getSubject().getSubjectConfirmations().get(0);
            SubjectConfirmationData scd = sc.getSubjectConfirmationData();
            scd.setNotOnOrAfter(DateTime.now().plus(60000));
            scd.setRecipient(getSSOEndpointUrl());
            assertion.getAuthnStatements().get(0).setAuthnInstant(DateTime.now());

            assertion.getConditions().getAudienceRestrictions().get(0).getAudiences().get(0).setAudienceURI( //
                    SAMLUtils.getEntityIdFromTenantId(getGlobalAuthTestBed().getMainTestTenant().getId()));
            assertion.getSubject().getNameID()
                    .setValue(getGlobalAuthTestBed().getCurrentUser().getResult().getUser().getEmailAddress());
            setResponseIssuedBy(response, idp);
            return response;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void setResponseIssuedBy(Response response, IdentityProvider identityProvider) {
        response.getIssuer().setValue(identityProvider.getEntityId());
        response.getAssertions().get(0).getIssuer().setValue(identityProvider.getEntityId());
    }

    public void signResponse(Response response) {
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

    public String getSSOEndpointUrl() {
        return baseUrl + "/SSO/alias/" + getGlobalAuthTestBed().getMainTestTenant().getId();
    }

    public RestTemplate getSamlRestTemplate() {
        RestTemplate restTemplate = HttpClientUtils.newRestTemplate();
        HttpMessageConverter<?> formHttpMessageConverter = new FormHttpMessageConverter();
        HttpMessageConverter<?> stringHttpMessageConverter = new StringHttpMessageConverter();
        restTemplate.getMessageConverters().add(formHttpMessageConverter);
        restTemplate.getMessageConverters().add(stringHttpMessageConverter);
        return restTemplate;
    }

    public IdentityProvider constructIdp() {
        IdentityProvider idp = new IdentityProvider();
        idp.setEntityId("http://testidp.lattice-engines.com/" + UUID.randomUUID().toString());
        String metadata = generateMetadata(idp.getEntityId());
        idp.setMetadata(metadata);
        return idp;
    }

    public String generateMetadata(String entityId) {
        try (InputStream stream = getClass().getResourceAsStream(RESOURCE_BASE + "idp_metadata.xml")) {
            EntityDescriptor descriptor = (EntityDescriptor) SAMLUtils.deserialize(parserPool, stream);
            descriptor.setEntityID(entityId);
            return SAMLUtils.serialize(descriptor);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public ResponseEntity<Void> sendSamlResponse(Response response) {
        return sendSamlResponse(response, true);
    }

    public ResponseEntity<Void> sendSamlResponse(Response response, boolean sign) {
        if (sign) {
            signResponse(response);
        }
        String xml = SAMLUtils.serialize(response);
        String encoded = Base64.encodeBytes(xml.getBytes());
        MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
        map.add("SAMLResponse", encoded);
        String url = getSSOEndpointUrl();
        ResponseEntity<Void> httpResponse = getSamlRestTemplate().postForEntity(url, map, Void.class);
        return httpResponse;
    }

}
