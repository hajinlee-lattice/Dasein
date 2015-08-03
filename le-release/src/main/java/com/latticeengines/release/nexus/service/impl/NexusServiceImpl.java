package com.latticeengines.release.nexus.service.impl;

import java.nio.charset.Charset;
import java.util.Arrays;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.converter.FormHttpMessageConverter;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.release.nexus.service.NexusService;
import com.latticeengines.release.resttemplate.util.AuthorizationHeaderHttpRequestInterceptor;
import com.latticeengines.release.resttemplate.util.LoggingHttpRequestInterceptor;
import com.latticeengines.release.resttemplate.util.RestTemplateUtil;

@Service("nexusService")
public class NexusServiceImpl implements NexusService {

    @Autowired
    private RestTemplate restTemplate;

    @Value("${release.nexus.url}")
    private String nexusUrl;
    
    @Value("${release.nexus.user.credential}")
    private String creds;

    @Override
    public ResponseEntity<String> uploadArtifactToNexus(String url, String project, String version) {
        AuthorizationHeaderHttpRequestInterceptor authInterceptor = new AuthorizationHeaderHttpRequestInterceptor(
                RestTemplateUtil.encodeToken(creds));
        LoggingHttpRequestInterceptor loggingInterceptor = new LoggingHttpRequestInterceptor();
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { authInterceptor, loggingInterceptor }));

        FormHttpMessageConverter formConverter = new FormHttpMessageConverter();
        formConverter.setCharset(Charset.forName("UTF8"));
        restTemplate.getMessageConverters().add(formConverter);

        MultiValueMap<String, Object> parts = createMultipartFormData(project, version);
 
        return restTemplate.postForEntity(nexusUrl, parts, String.class);
    }

    private MultiValueMap<String, Object> createMultipartFormData(String project, String version){
        MultiValueMap<String, Object> parts = new LinkedMultiValueMap<String, Object>();
        parts.add("hasPom", "false");
        parts.add("r", "releases");
        parts.add("g", "com.latticeengines");
        parts.add("a", project); //project
        parts.add("v", version); //version
        parts.add("p", "jar");

        FileSystemResource file = new FileSystemResource("/home/hliu/workspace6/ledp/le-pls/target/le-pls-2.0.5-SNAPSHOT.jar");
        System.out.println(file.exists());
        parts.add("file", file);
        return parts;
    }
}
