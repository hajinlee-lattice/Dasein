package com.latticeengines.eai.yarn.runtime;

import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataplatform.exposed.yarn.runtime.SingleContainerYarnProcessor;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.route.CamelRouteConfiguration;
import com.latticeengines.domain.exposed.eai.route.SftpToHdfsRouteConfiguration;
import com.latticeengines.eai.service.CamelRouteService;
import com.latticeengines.eai.service.impl.camel.SftpToHdfsRouteService;

@Component("camelRouteProcessor")
public class CamelRouteProcessor extends SingleContainerYarnProcessor<ImportConfiguration>
        implements ItemProcessor<ImportConfiguration, String> {

    private static final Long timeout = TimeUnit.HOURS.toMillis(48);
    private static final Log log = LogFactory.getLog(CamelRouteProcessor.class);

    @Autowired
    private SftpToHdfsRouteService sftpToHdfsRouteService;

    @Override
    public String process(ImportConfiguration importConfig) throws Exception {
        if (!ImportConfiguration.ImportType.CamelRoute.equals(importConfig.getImportType())) {
            throw new IllegalArgumentException("An import of type " + importConfig.getImportType() + " was directed to "
                    + this.getClass().getSimpleName());
        }

        System.out.println(JsonUtils.serialize(importConfig.getCamelRouteConfiguration()));

        CamelRouteConfiguration camelRouteConfiguration = importConfig.getCamelRouteConfiguration();
        CamelContext camelContext = new DefaultCamelContext();
        CamelRouteService<?> camelRouteService;

        if (camelRouteConfiguration instanceof SftpToHdfsRouteConfiguration) {
            camelRouteService = sftpToHdfsRouteService;
        } else {
            throw new UnsupportedOperationException(
                    camelRouteConfiguration.getClass().getSimpleName() + " has not been implemented yet.");
        }

        RouteBuilder route = camelRouteService.generateRoute(camelRouteConfiguration);
        camelContext.addRoutes(route);
        camelContext.start();
        waitForRouteToFinish(camelRouteService, camelRouteConfiguration);
        camelContext.stop();

        return null;
    }

    private void waitForRouteToFinish(CamelRouteService<?> camelRouteService,
            CamelRouteConfiguration camelRouteConfiguration) {
        Long startTime = System.currentTimeMillis();
        Integer errorTimes = 0;
        while (System.currentTimeMillis() - startTime < timeout) {
            try {
                if (camelRouteService.routeIsFinished(camelRouteConfiguration)) {
                    setProgress(0.95f);
                    return;
                } else {
                    String msg = "Waiting for the camel route to finish";
                    Double progress = camelRouteService.getProgress(camelRouteConfiguration);
                    if (progress != null) {
                        setProgress(progress.floatValue());
                        msg += ": " + progress * 100 + "%";
                    }
                    log.info(msg);
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                if (++errorTimes >= 10) {
                    throw new RuntimeException("Max error times exceeded: encountered " + errorTimes + " errors.", e);
                }
            } finally {
                try {
                    Thread.sleep(5000L);
                } catch (InterruptedException e) {
                    log.error(e);
                    // ignore
                }
            }
        }
    }

}
