package com.latticeengines.eai.yarn.runtime;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.domain.exposed.eai.EaiJobConfiguration;
import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.route.CamelRouteConfiguration;
import com.latticeengines.yarn.exposed.runtime.SingleContainerYarnProcessor;

public class EaiProcessor extends SingleContainerYarnProcessor<EaiJobConfiguration>
        implements ItemProcessor<EaiJobConfiguration, String> {

    private static final Log log = LogFactory.getLog(EaiProcessor.class);

    @Autowired
    private ImportProcessor importProcessor;

    @Autowired
    private ExportProcessor exportProcessor;

    @Autowired
    private CamelRouteProcessor camelRouteProcessor;

    @Override
    public String process(EaiJobConfiguration eaiJobConfig) throws Exception {
        if (eaiJobConfig instanceof ImportConfiguration) {
            log.info("Directing import job to " + importProcessor.getClass().getSimpleName());
            return importProcessor.process((ImportConfiguration) eaiJobConfig);
        } else if (eaiJobConfig instanceof ExportConfiguration) {
            return exportProcessor.process((ExportConfiguration) eaiJobConfig);
        } else if (eaiJobConfig instanceof CamelRouteConfiguration) {
            log.info("Directing import job to " + camelRouteProcessor.getClass().getSimpleName());
            return camelRouteProcessor.process((CamelRouteConfiguration) eaiJobConfig);
        }
        return null;
    }
}
