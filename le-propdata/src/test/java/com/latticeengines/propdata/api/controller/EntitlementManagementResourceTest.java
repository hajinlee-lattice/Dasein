package com.latticeengines.propdata.api.controller;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.web.client.RestTemplate;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.DataColumnMap;
import com.latticeengines.domain.exposed.propdata.EntitlementPackages;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsPackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsPackages;
import com.latticeengines.domain.exposed.propdata.EntitlementSourcePackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourcePackages;
import com.latticeengines.domain.exposed.propdata.ResponseID;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-propdata-context.xml" })
public class EntitlementManagementResourceTest extends AbstractTestNGSpringContextTests{
	
	@Value("${propdata.api.hostport}")
    private String hostPort;
	
	protected String getRestAPIHostPort() {
        return hostPort;
    }

	private RestTemplate restTemplate = new RestTemplate();
	
	@SuppressWarnings("unchecked")
	@Test(groups = "functional")
	public void testDerivedAttributePackage() {
		Object sourcePackageName = new String("Test");
		Object sourcePackageDescription = new String("Test");
		Object isDefault = new Boolean(false);
		HttpHeaders headers = new HttpHeaders();
	    headers.add("Content-Type", "application/json");
	    headers.add("Accept", "application/json");
	    HttpEntity<String> requestEntity = new HttpEntity<>("", headers);
		ResponseEntity<ResponseID> packageID = restTemplate.exchange(getRestAPIHostPort() 
				+ "/PropData/entitlements/source/", HttpMethod.PUT
				,requestEntity,ResponseID.class,sourcePackageName,sourcePackageDescription
				,isDefault);
	    assertNotNull(packageID);
	    
	    List<EntitlementPackages> packages = restTemplate.getForObject(getRestAPIHostPort() 
	    		+ "/PropData/entitlements/derived/",List.class);
	    assertNotNull(packages);
	    
	    Object sourceTableName = new String("Alexa_Source");
		Object extensionName = new String("Language");
		ResponseEntity<ResponseID> columnID = restTemplate.exchange(
				getRestAPIHostPort() + "/PropData/entitlements/derived/column/" 
				+ packageID.getBody().getID(), HttpMethod.PUT
				,requestEntity,ResponseID.class,extensionName,sourceTableName);
	    assertNotNull(columnID);
	    
	    List<DataColumnMap> columns = restTemplate.getForObject(getRestAPIHostPort() 
	    		+ "/PropData/entitlements/derived/details/" 
	    		+ packageID.getBody().getID(),List.class);
	    assertNotNull(columns);
	    
	    Object contractID = new String("Test");
		ResponseEntity<ResponseID> customerID = restTemplate.exchange(
				getRestAPIHostPort() + "/PropData/entitlements/derived/customer/" 
				+ packageID.getBody().getID() + "/" + contractID, HttpMethod.PUT
				,requestEntity,ResponseID.class);
	    assertNotNull(customerID);
	    
	    packages = restTemplate.getForObject(getRestAPIHostPort() 
	    		+ "/PropData/entitlements/derived/Test",List.class);
	    assertNotNull(packages);
	      
	    packages = restTemplate.getForObject(getRestAPIHostPort() 
	    		+ "/PropData/entitlements/derived/abc",List.class);
	    assertNull(packages);
	    
		restTemplate.delete(
				getRestAPIHostPort() + "/PropData/entitlements/derived/customer/" 
				+ packageID.getBody().getID() + "/" + contractID);

		restTemplate.delete(
				getRestAPIHostPort() + "/PropData/entitlements/derived/column/" 
				+ packageID.getBody().getID()
				,extensionName,sourceTableName);
	
	}
	
	@SuppressWarnings("unchecked")
	@Test(groups = "functional")
	public void createSourcePackage() {
		Object packageName = new String("Test");
		Object packageDescription = new String("Test");
		Object isDefault = new Boolean(false);
		HttpHeaders headers = new HttpHeaders();
	    headers.add("Content-Type", "application/json");
	    headers.add("Accept", "application/json");
	    HttpEntity<String> requestEntity = new HttpEntity<>("", headers);
		ResponseEntity<ResponseID> packageID = restTemplate.exchange(getRestAPIHostPort() 
				+ "/PropData/entitlements/derived/", HttpMethod.PUT
				,requestEntity,ResponseID.class,packageName,packageDescription,isDefault);
	    assertNotNull(packageID);
	    
	    List<EntitlementSourcePackages> packages 
	    	= restTemplate.getForObject(getRestAPIHostPort() + "/PropData/entitlements/source/"
	    	,List.class);
	    assertNotNull(packages);
	    
	    Object lookupID = new String("Alexa_Source");
		ResponseEntity<ResponseID> sourceID = restTemplate.exchange(
				getRestAPIHostPort() + "/PropData/source/source/" + packageID.getBody().getID()
				, HttpMethod.PUT,requestEntity,ResponseID.class,lookupID);
	    assertNotNull(sourceID);
	    
	    List<EntitlementSourcePackageMap> sourcePackageMap 
		    = restTemplate.getForObject(getRestAPIHostPort() 
		    + "/PropData/entitlements/source/details/" + packageID.getBody().getID(),List.class);
	    assertNotNull(sourcePackageMap);
	    
	    Object contractID = new String("Test");
		ResponseEntity<ResponseID> customerID = restTemplate.exchange(
				getRestAPIHostPort() + "/source/customer/" + packageID.getBody().getID() 
				+ "/" + contractID, HttpMethod.PUT,requestEntity,ResponseID.class);
	    assertNotNull(customerID);
	    
	    packages = restTemplate.getForObject(getRestAPIHostPort() 
	    		+ "/PropData/entitlements/source/Test",List.class);
	    assertNotNull(packages);
	      
	    packages = restTemplate.getForObject(getRestAPIHostPort() 
	    		+ "/PropData/entitlements/source/abc",List.class);
	    assertNull(packages);
	    
		restTemplate.delete(
				getRestAPIHostPort() + "/PropData/entitlements/source/customer/" 
				+ packageID.getBody().getID() + "/" + contractID);

		restTemplate.delete(
				getRestAPIHostPort() + "/PropData/entitlements/derived/column/" 
				+ packageID.getBody().getID(),lookupID);
	}
	
	@SuppressWarnings("unchecked")
	@Test(groups = "functional")
	public void createSourceColumnPackage() {
		  Object sourceColumnPackageName = new String("Test");
		  Object sourceColumnPackageDescription = new String("Test");
		  Object isDefault = new Boolean(false);
		  HttpHeaders headers = new HttpHeaders();
	      headers.add("Content-Type", "application/json");
	      headers.add("Accept", "application/json");
	      HttpEntity<String> requestEntity = new HttpEntity<>("", headers);
		  ResponseEntity<ResponseID> packageID = restTemplate.exchange(getRestAPIHostPort() 
				  + "/PropData/entitlements/columns/", HttpMethod.PUT
				  ,requestEntity,ResponseID.class,sourceColumnPackageName
				  ,sourceColumnPackageDescription,isDefault);
	      assertNotNull(packageID);
	      
	      List<EntitlementSourceColumnsPackages> packages 
	      		= restTemplate.getForObject(getRestAPIHostPort() + "/PropData/entitlements/columns/"
	      		,List.class);
	      assertNotNull(packages);
	      
	      Object lookupID = new String("Alexa_Source");
		  Object columnName = new String("Language");
		  ResponseEntity<ResponseID> columnID = restTemplate.exchange(
				  getRestAPIHostPort() + "/PropData/columns/column/" + packageID.getBody().getID()
				  , HttpMethod.PUT,requestEntity,ResponseID.class,lookupID,columnName);
	      assertNotNull(columnID);
	      
	      List<EntitlementSourceColumnsPackageMap> columns 
	      		= restTemplate.getForObject(getRestAPIHostPort() 
	      		+ "/PropData/entitlements/columns/details/" + packageID.getBody().getID()
	      		,List.class);
	      assertNotNull(columns);
	      
	      Object contractID = new String("Test");
		  ResponseEntity<ResponseID> customerID = restTemplate.exchange(
				getRestAPIHostPort() + "/columns/customer/" + packageID.getBody().getID() + "/" 
				+ contractID, HttpMethod.PUT
				,requestEntity,ResponseID.class);
	      assertNotNull(customerID);
	      
	      packages = restTemplate.getForObject(getRestAPIHostPort() 
	    		  + "/PropData/entitlements/columns/Test",List.class);
	      assertNotNull(packages);
	      
	      packages = restTemplate.getForObject(getRestAPIHostPort() 
	    		  + "/PropData/entitlements/columns/abc",List.class);
	      assertNull(packages);
	      
		  restTemplate.delete(
				getRestAPIHostPort() + "/PropData/entitlements/columns/customer/" 
				+ packageID.getBody().getID() + "/" + contractID);
		  
		  restTemplate.delete(
				getRestAPIHostPort() + "/PropData/entitlements/derived/column/" 
				+ packageID.getBody().getID(),lookupID,columnName);
	}
}
