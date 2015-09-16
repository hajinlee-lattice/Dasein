package com.latticeengines.propdata.api.controller;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.propdata.DataColumnMap;
import com.latticeengines.domain.exposed.propdata.EntitlementPackages;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsPackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsPackages;
import com.latticeengines.domain.exposed.propdata.EntitlementSourcePackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourcePackages;
import com.latticeengines.propdata.api.testframework.PropDataApiFunctionalTestNGBase;

import java.util.List;

public class EntitlementManagementResourceTestNG extends PropDataApiFunctionalTestNGBase {
    
    @Value("${propdata.api.functional.hostport}")
    private String hostPort;
    
    protected String getRestAPIHostPort() {
        return hostPort;
    }

    private RestTemplate restTemplate = new RestTemplate();
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test(groups = "disabled")
    public void testDerivedAttributePackage() {
        Object sourcePackageName = new String("Test");
        Object sourcePackageDescription = new String("Test");
        Object isDefault = new Boolean(false);
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");
        headers.add("Accept", "application/json");
        HttpEntity<String> requestEntity = new HttpEntity<>("", headers);
        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(getRestAPIHostPort() 
            + "/PropData/entitlements/derived/")
            .queryParam("packageName", sourcePackageName)
            .queryParam("packageDescription", sourcePackageDescription)
            .queryParam("isDefault", isDefault);
        ResponseEntity<ResponseDocument> packageID = restTemplate.exchange(builder.build().encode().toUri()
            ,HttpMethod.PUT,requestEntity,ResponseDocument.class);
        assertNotNull(packageID);
        
        List<EntitlementPackages> packages = restTemplate.getForObject(getRestAPIHostPort()
            + "/PropData/entitlements/derived/",List.class);
        assertFalse(packages.isEmpty());
        
        Object sourceTableName = new String("Alexa_Source");
        Object extensionName = new String("AlexaLanguage");
        builder = UriComponentsBuilder.fromHttpUrl(getRestAPIHostPort() 
            + "/PropData/entitlements/derived/column/" + packageID.getBody().getResult())
            .queryParam("sourceTableName", sourceTableName)
            .queryParam("extensionName", extensionName);
        ResponseEntity<ResponseDocument> columnID = restTemplate.exchange(
            builder.build().encode().toUri(), HttpMethod.PUT
            ,requestEntity,ResponseDocument.class);
        assertNotNull(columnID);
        
        List<DataColumnMap> columns = restTemplate.getForObject(getRestAPIHostPort()
            + "/PropData/entitlements/derived/details/" 
            + packageID.getBody().getResult(),List.class);
        assertFalse(columns.isEmpty());
        
        Object contractID = new String("Test");
        ResponseEntity<ResponseDocument> customerID = restTemplate.exchange(
            getRestAPIHostPort() + "/PropData/entitlements/derived/customer/" 
            + packageID.getBody().getResult() + "/" + contractID, HttpMethod.PUT
            ,requestEntity,ResponseDocument.class);
        assertNotNull(customerID);
        
        packages = restTemplate.getForObject(getRestAPIHostPort() 
            + "/PropData/entitlements/derived/Test",List.class);
        assertFalse(packages.isEmpty());
          
        packages = restTemplate.getForObject(getRestAPIHostPort() 
            + "/PropData/entitlements/derived/abc",List.class);
        assertTrue(packages.isEmpty());
        
        restTemplate.delete(
            getRestAPIHostPort() + "/PropData/entitlements/derived/customer/" 
            + packageID.getBody().getResult() + "/" + contractID);

        builder = UriComponentsBuilder.fromHttpUrl(getRestAPIHostPort() 
            + "/PropData/entitlements/derived/column/" 
            + packageID.getBody().getResult())
            .queryParam("extensionName", extensionName)
            .queryParam("sourceTableName", sourceTableName);
        restTemplate.delete(
            builder.build().encode().toUri());
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test(groups = "disabled")
    public void createSourcePackage() {
        Object packageName = new String("Test");
        Object packageDescription = new String("Test");
        Object isDefault = new Boolean(false);
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");
        headers.add("Accept", "application/json");
        HttpEntity<String> requestEntity = new HttpEntity<>("", headers);
        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(getRestAPIHostPort() 
            + "/PropData/entitlements/source/")
            .queryParam("sourcePackageName", packageName)
            .queryParam("sourcePackageDescription", packageDescription)
            .queryParam("isDefault", isDefault);
        ResponseEntity<ResponseDocument> packageID = restTemplate.exchange(
            builder.build().encode().toUri(), HttpMethod.PUT
            ,requestEntity,ResponseDocument.class);
        assertNotNull(packageID);
        
        List<EntitlementSourcePackages> packages
            = restTemplate.getForObject(getRestAPIHostPort() + "/PropData/entitlements/source/"
            ,List.class);
        assertFalse(packages.isEmpty());
        
        Object lookupID = new String("Alexa_Source");
        builder = UriComponentsBuilder.fromHttpUrl(getRestAPIHostPort() 
            + "/PropData/entitlements/source/source/" + packageID.getBody().getResult())
            .queryParam("lookupID", lookupID);
        ResponseEntity<ResponseDocument> sourceID = restTemplate.exchange(
            builder.build().encode().toUri()
            , HttpMethod.PUT,requestEntity,ResponseDocument.class);
        assertNotNull(sourceID);
            List<EntitlementSourcePackageMap> sourcePackageMap
            = restTemplate.getForObject(getRestAPIHostPort() 
            + "/PropData/entitlements/source/details/" + packageID.getBody().getResult(),List.class);
        assertFalse(sourcePackageMap.isEmpty());
        
        Object contractID = new String("Test");
        ResponseEntity<ResponseDocument> customerID = restTemplate.exchange(
            getRestAPIHostPort() + "/PropData/entitlements/source/customer/" + packageID.getBody().getResult() 
            + "/" + contractID, HttpMethod.PUT,requestEntity,ResponseDocument.class);
        assertNotNull(customerID);
        
        packages = restTemplate.getForObject(getRestAPIHostPort() 
            + "/PropData/entitlements/source/Test",List.class);
        assertFalse(packages.isEmpty());
          
        packages = restTemplate.getForObject(getRestAPIHostPort() 
            + "/PropData/entitlements/source/abc",List.class);
        assertTrue(packages.isEmpty());
        
        restTemplate.delete(
            getRestAPIHostPort() + "/PropData/entitlements/source/customer/" 
            + packageID.getBody().getResult() + "/" + contractID);

        builder = UriComponentsBuilder.fromHttpUrl(getRestAPIHostPort() 
            + "/PropData/entitlements/source/source/" 
            + packageID.getBody().getResult())
            .queryParam("lookupID", lookupID);
        restTemplate.delete(
            builder.build().encode().toUri());
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test(groups = "disabled")
    public void createSourceColumnPackage() {
        Object sourceColumnPackageName = new String("Test");
        Object sourceColumnPackageDescription = new String("Test");
        Object isDefault = new Boolean(false);
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");
        headers.add("Accept", "application/json");
        HttpEntity<String> requestEntity = new HttpEntity<>("", headers);
        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(getRestAPIHostPort() 
            + "/PropData/entitlements/columns/")
            .queryParam("sourceColumnPackageName", sourceColumnPackageName)
            .queryParam("sourceColumnPackageDescription", sourceColumnPackageDescription)
            .queryParam("isDefault", isDefault);
        ResponseEntity<ResponseDocument> packageID = restTemplate.exchange(
            builder.build().encode().toUri(), HttpMethod.PUT
            ,requestEntity,ResponseDocument.class);
        assertNotNull(packageID);
          
        List<EntitlementSourceColumnsPackages> packages
            = restTemplate.getForObject(getRestAPIHostPort() + "/PropData/entitlements/columns/"
            ,List.class);
        assertFalse(packages.isEmpty());
          
        Object lookupID = new String("Alexa_Source");
        Object columnName = new String("Language");
        builder = UriComponentsBuilder.fromHttpUrl(getRestAPIHostPort() 
            + "/PropData/entitlements/columns/column/" + packageID.getBody().getResult())
            .queryParam("lookupID", lookupID)
            .queryParam("columnName", columnName);
        ResponseEntity<ResponseDocument> columnID = restTemplate.exchange(
            builder.build().encode().toUri()
            ,HttpMethod.PUT,requestEntity,ResponseDocument.class);
        assertNotNull(columnID);
          
        List<EntitlementSourceColumnsPackageMap> columns
            = restTemplate.getForObject(getRestAPIHostPort() 
            + "/PropData/entitlements/columns/details/" + packageID.getBody().getResult()
            ,List.class);
        assertFalse(columns.isEmpty());
          
        Object contractID = new String("Test");
        ResponseEntity<ResponseDocument> customerID = restTemplate.exchange(
            getRestAPIHostPort() + "/PropData/entitlements/columns/customer/" + packageID.getBody().getResult() + "/" 
            + contractID, HttpMethod.PUT
            ,requestEntity,ResponseDocument.class);
        assertNotNull(customerID);
          
        packages = restTemplate.getForObject(getRestAPIHostPort() 
            + "/PropData/entitlements/columns/Test",List.class);
            assertFalse(packages.isEmpty());
          
        packages = restTemplate.getForObject(getRestAPIHostPort() 
            + "/PropData/entitlements/columns/abc",List.class);
        assertTrue(packages.isEmpty());
          
        restTemplate.delete(
            getRestAPIHostPort() + "/PropData/entitlements/columns/customer/" 
            + packageID.getBody().getResult() + "/" + contractID);
          
        builder = UriComponentsBuilder.fromHttpUrl(getRestAPIHostPort() 
            + "/PropData/entitlements/columns/column/" 
            + packageID.getBody().getResult())
            .queryParam("lookupID", lookupID)
            .queryParam("columnName", columnName);
        restTemplate.delete(
        builder.build().encode().toUri());
    }
}
