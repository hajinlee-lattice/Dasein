package com.latticeengines.playmaker.exception;

import org.springframework.http.ResponseEntity;
import org.springframework.security.oauth2.common.exceptions.OAuth2Exception;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ExceptionEncodingTranslatorTestNG {

    @Test(groups = "unit")
    public void translate() throws Exception {
        ExceptionEncodingTranslator translator = new ExceptionEncodingTranslator();
        Exception inputException = new Exception("Invaid Token:abc<script>Alert('hi')</script>efg");
        ResponseEntity<OAuth2Exception> output = translator.translate(inputException);
        Exception outputException = output.getBody();
        Assert.assertEquals(outputException.getMessage(),
                "Invaid Token:abc&lt;script&gt;Alert(&#39;hi&#39;)&lt;/script&gt;efg");
    }
}
