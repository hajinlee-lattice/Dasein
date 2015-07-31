package com.latticeengines.release.jenkins.service.impl;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.client.support.HttpRequestWrapper;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriTemplate;
import org.apache.commons.codec.binary.Base64;
import com.latticeengines.release.jenkins.service.JenkinsService;


@Service("jenkinsService")
public class JenkinsServiceImpl implements JenkinsService{

    @Value("${release.jenkins.deploymenttest.url}")
    private String deploymentTestUrl;

    @Value("${release.jenkins.user.credential}")
    private String creds;

    @Autowired
    private RestTemplate restTemplate;

    @Override
    public ResponseEntity<String> triggerJenkinsJob() {
        String plainCreds = "release:tahoe";
        byte[] plainCredsBytes = plainCreds.getBytes();
        byte[] base64CredsBytes = Base64.encodeBase64(plainCredsBytes);
        String base64Creds = new String(base64CredsBytes);
        AuthorizationHeaderHttpRequestInterceptor interceptor = new AuthorizationHeaderHttpRequestInterceptor("");
        interceptor.setAuthValue(base64Creds);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{interceptor}));
        
        Parameter p = new Parameter();
        KV k1 = new KV();
        k1.setName("SVN_DIR");
        k1.setValue("tags");
        KV k2 = new KV();
        k2.setName("SVN_BRANCH_NAME");
        k2.setValue("release_2.0.4");
        
        
        p.setPar(Arrays.asList(new KV[]{k1, k2}));
        
        
        URI expanded = new UriTemplate(deploymentTestUrl).expand(p); // this is what RestTemplate uses 
        try {
            deploymentTestUrl = URLDecoder.decode(expanded.toString(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } // java.net class
        
        
        return restTemplate.postForEntity(deploymentTestUrl, "", String.class, new Object());
    }

    public static class AuthorizationHeaderHttpRequestInterceptor implements ClientHttpRequestInterceptor {

        private String headerValue;

        public AuthorizationHeaderHttpRequestInterceptor(String headerValue) {
            this.headerValue = headerValue;
        }

        @Override
        public ClientHttpResponse intercept(HttpRequest request, byte[] body, ClientHttpRequestExecution execution)
                throws IOException {
            HttpRequestWrapper requestWrapper = new HttpRequestWrapper(request);
            requestWrapper.getHeaders().add("Authorization", "Basic " + headerValue);

            return execution.execute(requestWrapper, body);
        }

        public void setAuthValue(String headerValue) {
            this.headerValue = headerValue;
        }
    }
    
    public static class Parameter{
        
        List<KV> par = new ArrayList<>();
        
        public List<KV> getPar(){
            return par;
        }

        public void setPar(List<KV> par){
            this.par = par;
        }
        
    }

    public static class KV{
        private String name;

        private String value;

        public String getName(){
            return this.name;
        }

        public void setName(String name){
            this.name = name;
        }

        public String getValue(){
            return this.value;
        }

        public void setValue(String value){
            this.value = value;
        }
    }

}
