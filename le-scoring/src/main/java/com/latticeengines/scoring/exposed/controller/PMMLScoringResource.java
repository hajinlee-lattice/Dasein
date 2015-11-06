//package com.latticeengines.scoring.exposed.controller;
//
//import java.io.InputStream;
//import java.util.List;
//
//import javax.servlet.http.HttpServletRequest;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.dmg.pmml.IOUtil;
//import org.dmg.pmml.PMML;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Controller;
//import org.springframework.web.bind.annotation.PathVariable;
//import org.springframework.web.bind.annotation.RequestBody;
//import org.springframework.web.bind.annotation.RequestMapping;
//import org.springframework.web.bind.annotation.RequestMethod;
//import org.springframework.web.bind.annotation.ResponseBody;
//import org.xml.sax.InputSource;
//
//import com.latticeengines.scoring.exposed.domain.ScoringRequest;
//import com.latticeengines.scoring.exposed.domain.ScoringResponse;
//import com.latticeengines.scoring.exposed.service.ScoringService;
//import com.latticeengines.scoring.registry.PMMLModelRegistry;
//
//@Controller
//public class PMMLScoringResource {
//    private static final Log log = LogFactory.getLog(PMMLScoringResource.class);
//
//    @Autowired
//    private PMMLModelRegistry pmmlModelRegistry;
//
//    @Autowired
//    private ScoringService scoringService;
//
//    @RequestMapping( //
//    value = "/pmml/{id}", //
//    method = RequestMethod.PUT, //
//    headers = "Accept=application/xml, application/json", //
//    produces = "application/text")
//    @ResponseBody
//    public String deploy(@PathVariable String id, HttpServletRequest servletRequest) {
//        PMML pmml = null;
//        try {
//            InputStream is = servletRequest.getInputStream();
//
//            try {
//                pmml = IOUtil.unmarshal(new InputSource(is));
//                pmmlModelRegistry.put(id, pmml);
//            } finally {
//                is.close();
//            }
//        } catch (Exception e) {
//            log.error(e);
//        }
//        return "Model " + id + " deployed successfully.";
//    }
//
//    @RequestMapping( //
//    value = "/pmml/{id}", //
//    method = RequestMethod.DELETE, //
//    headers = "Accept=application/xml, application/json", //
//    produces = "application/text")
//    @ResponseBody
//    public String undeploy(@PathVariable String id) {
//        pmmlModelRegistry.remove(id);
//        return "Model " + id + " undeployed successfully";
//    }
//
//    @RequestMapping( //
//    value = "/pmml/{id}", //
//    method = RequestMethod.GET, //
//    headers = "Accept=application/xml, application/json")
//    @ResponseBody
//    public PMML getRegisteredModel(@PathVariable String id) {
//        return pmmlModelRegistry.get(id);
//    }
//    
//    @RequestMapping( //
//    value = "/pmml/{id}", //
//    method = RequestMethod.POST, //
//    headers = "Accept=application/json", //
//    produces = "application/json")
//    @ResponseBody
//    public ScoringResponse score(@PathVariable String id, @RequestBody ScoringRequest request) {
//        PMML pmml = pmmlModelRegistry.get(id);
//        if (pmml == null) {
//            throw new IllegalStateException("PMML model with id " + id + " has not yet been registered.");
//        }
//        return scoringService.score(request, pmml);
//    }
//
//    @RequestMapping( //
//    value = "/pmml/batch/{id}", //
//    method = RequestMethod.POST, //
//    headers = "Accept=application/json", //
//    produces = "application/json")
//    @ResponseBody
//    public List<ScoringResponse> scoreBatch(@PathVariable String id, @RequestBody List<ScoringRequest> requests) {
//        PMML pmml = pmmlModelRegistry.get(id);
//        if (pmml == null) {
//            throw new IllegalStateException("PMML model with id " + id + " has not yet been registered.");
//        }
//        return scoringService.scoreBatch(requests, pmml);
//    }
//    
//}
