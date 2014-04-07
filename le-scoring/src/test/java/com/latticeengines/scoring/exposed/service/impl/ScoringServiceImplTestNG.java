package com.latticeengines.scoring.exposed.service.impl;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.xml.transform.Source;

import org.dmg.pmml.PMML;
import org.jpmml.model.ImportFilter;
import org.jpmml.model.JAXBUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.xml.sax.InputSource;

import com.latticeengines.scoring.exposed.domain.ScoringRequest;
import com.latticeengines.scoring.functionalframework.ScoringFunctionalTestNGBase;

public class ScoringServiceImplTestNG extends ScoringFunctionalTestNGBase {

    private PMML pmml;
    private List<ScoringRequest> requests = new ArrayList<ScoringRequest>();
    
    @Autowired
    private ScoringServiceImpl scoringService;

    @BeforeClass(groups = "unit")
    public void setup() throws Exception {
        InputStream pmmlInputStream = ClassLoader
                .getSystemResourceAsStream("com/latticeengines/scoring/LogisticRegressionPMML.xml");
        Source source = ImportFilter.apply(new InputSource(pmmlInputStream));
        pmml = JAXBUtil.unmarshalPMML(source); 
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            ScoringRequest request = new ScoringRequest();
            Map<String, Object> params = new HashMap<String, Object>();
            params.put("age", 30.0 + random.nextDouble() * 10.0);
            params.put("salary", 65000 + random.nextDouble() * 10000.0);
            params.put("car_location", random.nextInt(2) == 0 ? "street" : "carpark");
            request.setArguments(params);
            requests.add(request);
        }
    }
    
    @Test(groups = "functional")
    public void scoreBatch() {
        long time1 = System.currentTimeMillis();
        scoringService.scoreBatch(requests, pmml);
        long time2 = System.currentTimeMillis() - time1;
        System.out.println("Batch scoring elapsed time = " + time2);
    }
    
    @Test(groups = "functional")
    public void score() {
        long time1 = System.currentTimeMillis();
        for (ScoringRequest request : requests) {
            scoringService.score(request, pmml);
        }
        long time2 = System.currentTimeMillis() - time1;
        System.out.println("Serial scoring elapsed time = " + time2);
    }
    
    
}
