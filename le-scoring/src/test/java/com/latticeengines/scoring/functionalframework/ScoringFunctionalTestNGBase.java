package com.latticeengines.scoring.functionalframework;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.scoring.exposed.domain.ScoringRequest;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-scoring-context.xml" })
public class ScoringFunctionalTestNGBase extends DataPlatformFunctionalTestNGBase {

    protected RestTemplate restTemplate = new RestTemplate();
    
    protected List<ScoringRequest> createListRequest(int numElements) {
        List<ScoringRequest> requests = new ArrayList<ScoringRequest>();
        Random random = new Random();
        for (int i = 0; i < numElements; i++) {
            ScoringRequest request = new ScoringRequest();
            Map<String, Object> params = new HashMap<String, Object>();
            params.put("age", 30.0 + random.nextDouble() * 10.0);
            params.put("salary", 65000 + random.nextDouble() * 10000.0);
            params.put("car_location", random.nextInt(2) == 0 ? "street" : "carpark");
            request.setArguments(params);
            requests.add(request);
        }
        return requests;
    }


}
