package com.latticeengines.eai.yarn.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.exposed.yarn.client.SingleContainerClientCustomization;

@Component("eaiClientCustomization")
public class EaiClientCustomization extends SingleContainerClientCustomization {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(EaiClientCustomization.class);

    @Autowired
    public EaiClientCustomization(Configuration yarnConfiguration,
            @Value("${dataplatform.yarn.job.basedir}") String hdfsJobBaseDir,
            @Value("${dataplatform.fs.web.defaultFS}") String webHdfs) {
        super(yarnConfiguration, hdfsJobBaseDir, webHdfs);
    }

    @Override
    public String getModuleName() {
        return "eai";
    }

}
