package com.latticeengines.eai.yarn.runtime;

import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.eai.route.CamelRouteConfiguration;
import com.latticeengines.domain.exposed.eai.route.SftpToHdfsRouteConfiguration;
import com.latticeengines.eai.service.CamelRouteService;
import com.latticeengines.eai.service.impl.camel.SftpToHdfsRouteService;
import com.latticeengines.yarn.exposed.runtime.SingleContainerYarnProcessor;

@Component("camelRouteProcessor")
public class CamelRouteProcessor extends SingleContainerYarnProcessor<CamelRouteConfiguration>
        implements ItemProcessor<CamelRouteConfiguration, String> {

    private static final Long timeout = TimeUnit.HOURS.toMillis(48);
    private static final Logger log = LoggerFactory.getLogger(CamelRouteProcessor.class);

    @Autowired
    private SftpToHdfsRouteService sftpToHdfsRouteService;

    @Override
    public String process(CamelRouteConfiguration camelRouteConfig) throws Exception {
        log.info(JsonUtils.serialize(camelRouteConfig));
        if (camelRouteConfig instanceof SftpToHdfsRouteConfiguration) {
            CamelContext camelContext = new DefaultCamelContext();
            CamelRouteService<?> camelRouteService = sftpToHdfsRouteService;
            RouteBuilder route = camelRouteService.generateRoute(camelRouteConfig);
            camelContext.addRoutes(route);
            camelContext.start();
            waitForRouteToFinish(camelRouteService, camelRouteConfig);
            camelContext.stop();
        } else {
            throw new UnsupportedOperationException(
                    camelRouteConfig.getClass().getSimpleName() + " has not been implemented yet.");
        }
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
                    log.error(e.getMessage(), e);
                    // ignore
                }
            }
        }
    }

}
