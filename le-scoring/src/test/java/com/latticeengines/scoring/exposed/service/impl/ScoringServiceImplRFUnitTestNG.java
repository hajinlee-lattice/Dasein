package com.latticeengines.scoring.exposed.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.InputStream;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import javax.xml.transform.Source;

import org.codehaus.jackson.map.ObjectMapper;
import org.dmg.pmml.PMML;
import org.jpmml.model.ImportFilter;
import org.jpmml.model.JAXBUtil;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.xml.sax.InputSource;

import com.latticeengines.scoring.exposed.domain.ScoringRequest;
import com.latticeengines.scoring.exposed.domain.ScoringResponse;

public class ScoringServiceImplRFUnitTestNG {

    private ScoringServiceImpl scoringService = new ScoringServiceImpl();
    private PMML pmml;
    

    @BeforeClass(groups = "unit")
    public void setup() throws Exception {
        InputStream pmmlInputStream = ClassLoader
                .getSystemResourceAsStream("com/latticeengines/scoring/rfOutput.xml");
        Source source = ImportFilter.apply(new InputSource(pmmlInputStream));
        pmml = JAXBUtil.unmarshalPMML(source); 
    }

    @Test(groups = "unit")
    public void score() {
        ScoringRequest request = new ScoringRequest();
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("Lead Source", 10f);
        params.put("Lead Source Detail", 10f);
        params.put("Country", 10f);
        params.put("Employees Form B", 20f);
        params.put("Territory", 20f);
        params.put("docinfo", 20f);
        params.put("LinksIn", 20f);
        params.put("javascript", 20f);
        params.put("IsManager", 1f);
        params.put("IsEmpty", 0f);
        params.put("framework", 20f);
        params.put("Web Server", 20f);
        params.put("analytics", 20f);
        params.put("ViewsRank", 20f);
        params.put("ReachRank", 20f);
        params.put("Rank", 20f);
        params.put("widgets", 20f);
        params.put("ViewsPerUser", 20f);
        params.put("ReachPerMillion", 20f);
        params.put("BusinessUrlNumPages", 20f);
        params.put("BusinessType", 20f);
        params.put("IsPublicDomain", 1f);
        
        request.setArguments(params);
        System.out.println(serialize(request));
        
        ScoringResponse response = scoringService.score(request, pmml);
        assertNotNull(response);
        assertEquals(response.getResult().size(), 1);
        assertEquals(response.getResult().keySet().iterator().next(), "P1_Event");
        System.out.println(serialize(response));
    }

    @Test(groups = "unit")
    public void scoreBatch() {
        //throw new RuntimeException("Test not implemented");
    }

    private static <T> String serialize(T object) {
        if (object == null) {
            return null;
        }
        ObjectMapper objectMapper = new ObjectMapper();
        StringWriter writer = new StringWriter();
        try {
            objectMapper.writeValue(writer, object);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return writer.toString();
    }
    
}
