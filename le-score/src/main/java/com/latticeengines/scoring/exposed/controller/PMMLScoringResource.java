package com.latticeengines.scoring.exposed.controller;

import java.io.InputStream;

import javax.servlet.http.HttpServletRequest;
import javax.xml.bind.JAXBException;
import javax.xml.transform.Source;

import org.dmg.pmml.PMML;
import org.jpmml.model.ImportFilter;
import org.jpmml.model.JAXBUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.xml.sax.InputSource;

import com.latticeengines.scoring.registry.PMMLModelRegistry;

@Controller
public class PMMLScoringResource {

    @Autowired
    private PMMLModelRegistry pmmlModelRegistry;
    
    public PMMLScoringResource() {
        try {
            JAXBUtil.getContext();
        } catch (JAXBException e) {
            throw new IllegalStateException("Issues getting JAXB context.");
        }
    }

    @RequestMapping(value = "/pmml/{id}", method = RequestMethod.PUT, headers = "Accept=application/xml, application/json")
    @ResponseBody
    public String deploy(@PathVariable String id, HttpServletRequest servletRequest) {
        PMML pmml = null;
        try {
            InputStream is = servletRequest.getInputStream();

            try {
                Source source = ImportFilter.apply(new InputSource(is));
                pmml = JAXBUtil.unmarshalPMML(source);
                pmmlModelRegistry.put(id, pmml);
            } finally {
                is.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "Model " + id + " deployed successfully.";
    }
    
    @RequestMapping(value = "/pmml/{id}", method = RequestMethod.GET, headers = "Accept=application/xml, application/json")
    @ResponseBody
    public PMML getRegisteredModel(@PathVariable String id) {
        return pmmlModelRegistry.get(id);
    }
}
