package com.latticeengines.eai.yarn.runtime;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataplatform.exposed.yarn.runtime.SingleContainerYarnProcessor;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.route.CamelRouteConfiguration;
import com.latticeengines.domain.exposed.eai.route.SftpToHdfsRouteConfiguration;
import com.latticeengines.eai.service.CamelRouteService;
import com.latticeengines.eai.service.impl.camel.SftpToHdfsRouteService;

@Component("camelRouteProcessor")
public class CamelRouteProcessor extends SingleContainerYarnProcessor<ImportConfiguration> implements
        ItemProcessor<ImportConfiguration, String>, ApplicationContextAware {

    private static final Long timeout = TimeUnit.HOURS.toMillis(48);
    private static final Log log = LogFactory.getLog(CamelRouteProcessor.class);

    private ApplicationContext applicationContext;
    private List<CamelRouteService<?>> camelRouteServices = new ArrayList<>();
    private List<CamelRouteConfiguration> camelRouteConfigurations;

    @Override
    public String process(ImportConfiguration importConfig) throws Exception {
        if (!ImportConfiguration.ImportType.CamelRoute.equals(importConfig.getImportType())) {
            throw new IllegalArgumentException("An import of type " + importConfig.getImportType()
                    + " was directed to " + this.getClass().getSimpleName());
        }

        camelRouteConfigurations = importConfig.getCamelRouteConfigurations();
        System.out.println(JsonUtils.serialize(camelRouteConfigurations));
        CamelContext camelContext = new DefaultCamelContext();

        for (CamelRouteConfiguration camelRouteConfiguration: camelRouteConfigurations) {
            CamelRouteService<?> camelRouteService;
            if (camelRouteConfiguration instanceof SftpToHdfsRouteConfiguration) {
                camelRouteService = (SftpToHdfsRouteService) applicationContext.getBean("sftpToHdfsRoutService");
            } else {
                throw new UnsupportedOperationException(camelRouteConfiguration.getClass().getSimpleName() + " has not been implemented yet.");
            }

            RouteBuilder route = camelRouteService.generateRoute(camelRouteConfiguration);
            camelContext.addRoutes(route);
            camelRouteServices.add(camelRouteService);
        }

        camelContext.start();
        waitForRouteToFinish();
        camelContext.stop();

        return null;
     }

    private void waitForRouteToFinish() {
        Long startTime = System.currentTimeMillis();
        Integer errorTimes = 0;
        int currentStep = 0;
        CamelRouteService<?> currentService;
        CamelRouteConfiguration currentConfiguration;
        while (System.currentTimeMillis() - startTime < timeout) {
            if (currentStep >= camelRouteConfigurations.size()) {
                break;
            }
            currentService = camelRouteServices.get(currentStep);
            currentConfiguration = camelRouteConfigurations.get(currentStep);
            try {
                if (currentService.routeIsFinished(currentConfiguration)) {
                    setProgress(convertToGlobalProgress(currentStep, 1.0f));
                    log.info("Step " + currentStep + "/" + camelRouteConfigurations.size() + " finished.");
                    // reset for next step
                    errorTimes = 0;
                    currentStep++;
                } else {
                    Double progress = currentService.getProgress(currentConfiguration);
                    setProgress(convertToGlobalProgress(currentStep, progress.floatValue()));
                    log.info("Waiting for the camel route to finish: " + progress * 100 + " % of step "
                            + currentStep + "/" + camelRouteConfigurations.size());
                }
            } catch (Exception e) {
                log.error(e);
                if (++errorTimes >= 10) {
                    throw new RuntimeException("Max error times exceeded: encountered " + errorTimes + " errors.");
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

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    private float convertToGlobalProgress(int step, float stepProgress) {
        int totalSteps = camelRouteConfigurations.size();
        if (totalSteps <= 0) {
            throw new IllegalStateException("Total number of steps is " + totalSteps);
        }

        float stepPortion = (float) step / totalSteps;
        return stepPortion * (step + 0.9f * stepProgress + 0.05f);
    }

}
