//package com.latticeengines.scoring.exposed.service.impl;
//
//import java.io.InputStream;
//import java.util.List;
//
//import org.dmg.pmml.IOUtil;
//import org.dmg.pmml.PMML;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.testng.annotations.BeforeClass;
//import org.testng.annotations.Test;
//import org.xml.sax.InputSource;
//
//import com.latticeengines.scoring.exposed.domain.ScoringRequest;
//import com.latticeengines.scoring.functionalframework.ScoringFunctionalTestNGBase;
//
//public class ScoringServiceImplTestNG extends ScoringFunctionalTestNGBase {
//
//    private PMML pmml;
//    private List<ScoringRequest> requests;
//    
//    @Autowired
//    private ScoringServiceImpl scoringService;
//
//    @BeforeClass(groups = "functional")
//    public void setup() throws Exception {
//        InputStream pmmlInputStream = ClassLoader.getSystemResourceAsStream("com/latticeengines/scoring/LogisticRegressionPMML.xml");
//        pmml = IOUtil.unmarshal(new InputSource(pmmlInputStream));
//        requests = createListRequest(150);
//    }
//    
//    @Test(groups = "functional")
//    public void scoreBatch() {
//        long time1 = System.currentTimeMillis();
//        scoringService.scoreBatch(requests, pmml);
//        long time2 = System.currentTimeMillis() - time1;
//        System.out.println("Batch scoring elapsed time = " + time2);
//    }
//    
//    @Test(groups = "functional")
//    public void score() {
//        long time1 = System.currentTimeMillis();
//        for (ScoringRequest request : requests) {
//            scoringService.score(request, pmml);
//        }
//        long time2 = System.currentTimeMillis() - time1;
//        System.out.println("Serial scoring elapsed time = " + time2);
//    }
//    
//    
//}
