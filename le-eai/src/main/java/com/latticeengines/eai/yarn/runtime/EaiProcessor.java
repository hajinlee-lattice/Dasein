package com.latticeengines.eai.yarn.runtime;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.latticeengines.dataplatform.exposed.yarn.runtime.SingleContainerYarnProcessor;
import com.latticeengines.domain.exposed.eai.EaiJobConfiguration;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.route.CamelRouteConfiguration;

public class EaiProcessor extends SingleContainerYarnProcessor<EaiJobConfiguration>
        implements ItemProcessor<EaiJobConfiguration, String>, ApplicationContextAware {

    private static final Log log = LogFactory.getLog(EaiProcessor.class);

    @Autowired
    private ImportTableProcessor importTableProcessor;

    @Autowired
    private CamelRouteProcessor camelRouteProcessor;

    @Override
    public String process(EaiJobConfiguration eaiJobConfig) throws Exception {
        if (eaiJobConfig instanceof ImportConfiguration) {
            log.info("Directing import job to " + importTableProcessor.getClass().getSimpleName());
            return importTableProcessor.process((ImportConfiguration)eaiJobConfig);
        } else if (eaiJobConfig instanceof CamelRouteConfiguration) {
            log.info("Directing import job to " + camelRouteProcessor.getClass().getSimpleName());
            return camelRouteProcessor.process((CamelRouteConfiguration)eaiJobConfig);
        }
        return null;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        importTableProcessor.setApplicationContext(applicationContext);
    }
}
