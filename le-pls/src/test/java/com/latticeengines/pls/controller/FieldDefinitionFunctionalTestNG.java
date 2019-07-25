package com.latticeengines.pls.controller;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.frontend.FetchFieldDefinitionsResponse;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.mchange.io.FileUtils;

public class FieldDefinitionFunctionalTestNG extends PlsFunctionalTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(FieldDefinitionFunctionalTestNG.class);

    @Autowired
    private BatonService batonService;

    @BeforeClass(groups = {"functional"})
    public void setup() throws Exception {
        super.setup();

    }

    @AfterClass(groups = {"functional"})
    public void teardown() throws Exception {

    }

    @Test(groups = {"functional"})
    public void testReadFile() {

        FetchFieldDefinitionsResponse fetchResponse = new FetchFieldDefinitionsResponse();
        String fetchResponseJson = "<failed response>";

        try {
            InputStream fetchResponseInputStream = ClassLoader.getSystemResourceAsStream(
                    "com/latticeengines/pls/controller/fetch-field-definition-response.json");
            if (fetchResponseInputStream != null) {
                fetchResponseJson = IOUtils.toString(fetchResponseInputStream, "UTF-8");
                log.error("FetchFieldDefinitionResponse (1) is:\n" + fetchResponseJson);
            } else {
                log.error("Fetch Response load method (1) failed.  Trying next method...");
            }
        } catch (IOException e) {
            log.error("Fetch Response load method (1) threw IOException error:", e);
            //log.error("Could not load mock response from resource");
        } catch (Exception e2) {
            log.error("Fetch Response load method (1) threw Exception " + e2.toString(), e2);
        }

        try {
            File fetchResponseFile = new File(ClassLoader
                    .getSystemResource(
                            "com/latticeengines/pls/controller/fetch-field-definition-response.json")
                    .getPath());
            if (fetchResponseFile != null) {
                fetchResponseJson = FileUtils.getContentsAsString(fetchResponseFile);
                log.error("FetchFieldDefinitionResponse (2) is:\n" + fetchResponseJson);
            } else {
                log.error("Fetch Response load method (2) failed.  Trying next method...");
            }
        } catch (IOException e) {
            log.error("Fetch Response load method (2) threw IOException error:", e);
            //log.error("Could not load mock response from resource");
        } catch (Exception e2) {
            log.error("Fetch Response load method (2) threw Exception " + e2.toString(), e2);
        }

        try {
            URL fetchResponseUrl = ClassLoader.getSystemResource(
                    "com/latticeengines/pls/controller/fetch-field-definition-response.json");
            if (fetchResponseUrl != null) {
                InputStream fetchResponseInputStream = new FileInputStream(new File(fetchResponseUrl.getPath()));
                fetchResponse = JsonUtils.deserialize(fetchResponseInputStream, FetchFieldDefinitionsResponse.class);

                log.error("FetchFieldDefinitionResponse (3) is:\n" + fetchResponse.toString());
            } else {
                log.error("Fetch Response load method (3) failed.  Trying next method...");
            }
        } catch (IOException e) {
            log.error("Fetch Response load method (3) threw IOException error:", e);
            //log.error("Could not load mock response from resource");
        } catch (Exception e2) {
            log.error("Fetch Response load method (3) threw Exception " + e2.toString(), e2);
        }

        try {
            File fetchResponseFile = new File(ClassLoader
                    .getSystemResource("com/latticeengines/pls/controller/fetch-field-definition-response.json")
                    .getFile());
            if (fetchResponseFile != null) {
                fetchResponseJson = org.apache.commons.io.FileUtils.readFileToString(fetchResponseFile,
                        Charset.defaultCharset());
                log.error("FetchFieldDefinitionResponse (4) is:\n" + fetchResponseJson);
            } else {
                log.error("Fetch Response load method (4) failed.  Trying next method...");
            }
        } catch (IOException e) {
            log.error("Fetch Response load method (4) threw IOException error:", e);
            //log.error("Could not load mock response from resource");
        } catch (Exception e2) {
            log.error("Fetch Response load method (4) threw Exception " + e2.toString(), e2);
        }

        try {
            InputStream fetchResponseInputStream = getClass().getClassLoader().getResourceAsStream(
                    "com/latticeengines/pls/controller/fetch-field-definition-response.json");
            //"com/latticeengines/pls/controller/internal/fetch-field-definition-response.json");
            if (fetchResponseInputStream != null) {
                fetchResponseJson = IOUtils.toString(fetchResponseInputStream, "UTF-8");
                log.error("FetchFieldDefinitionResponse (5) is:\n" + fetchResponseJson);
            } else {
                log.error("Fetch Response load method (5) failed.  Trying next method...");
            }
        } catch (IOException e) {
            log.error("Fetch Response load method (5) threw IOException error:", e);
            //log.error("Could not load mock response from resource");
        } catch (Exception e2) {
            log.error("Fetch Response load method (5) threw Exception " + e2.toString(), e2);
        }

        try {
            fetchResponse = JsonUtils.deserialize(fetchResponseJson, FetchFieldDefinitionsResponse.class);
        } catch (Exception e) {
            log.error("JSON deserialization step failed with error " + e.getMessage(), e);
        }

        if (fetchResponse != null) {
            log.error("FetchResponse is: " + fetchResponse.toString());
        }
    }

    @Test(groups = {"functional"})
    public void testReadS3() {


    }
}
