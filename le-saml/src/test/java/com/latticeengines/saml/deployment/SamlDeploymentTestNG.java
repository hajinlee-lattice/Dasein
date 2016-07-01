package com.latticeengines.saml.deployment;

import java.io.UnsupportedEncodingException;

import org.opensaml.saml2.core.Response;
import org.opensaml.xml.util.Base64;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.testng.annotations.Test;

import com.latticeengines.saml.util.SAMLUtils;

public class SamlDeploymentTestNG extends SamlDeploymentTestNGBase {

    @Test(groups = "deployment")
    public void testIdPInitiatedAuth() throws UnsupportedEncodingException {
        Response response = getTestSAMLResponse();
        String xml = SAMLUtils.serialize(response);
        String encoded = Base64.encodeBytes(xml.getBytes());
        MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
        map.add("SAMLResponse", encoded);
        String url = getSSOEndpointUrl();
        getSamlRestTemplate().postForObject(url, map, Void.class);
    }
}
