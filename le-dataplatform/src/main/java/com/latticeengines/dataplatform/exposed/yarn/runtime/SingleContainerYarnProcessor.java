package com.latticeengines.dataplatform.exposed.yarn.runtime;

import java.lang.reflect.ParameterizedType;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.context.ApplicationContext;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.swlib.SoftwarePackage;
import com.latticeengines.domain.exposed.swlib.SoftwarePackageInitializer;
import com.latticeengines.swlib.exposed.service.SoftwareLibraryService;

public abstract class SingleContainerYarnProcessor<T> implements ItemProcessor<T, String>, StepExecutionListener {

    private static final Log log = LogFactory.getLog(SingleContainerYarnProcessor.class);

    protected ApplicationId appId;
    private LineMapper<T> lineMapper = new SingleContainerLineMapper();
    private ItemWriter<String> itemWriter = new SingleContainerWriter();
    private Class<T> type;

    @SuppressWarnings("unchecked")
    public SingleContainerYarnProcessor() {
        this.type = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    public ApplicationContext loadSoftwarePackages(String module, SoftwareLibraryService softwareLibraryService, ApplicationContext context) {
        List<SoftwarePackage> packages = softwareLibraryService.getLatestInstalledPackages(module);
        log.info(String.format("Classpath = %s", System.getenv("CLASSPATH")));
        log.info(String.format("Found %d of software packages from the software library for this module.",
                packages.size()));
        for (SoftwarePackage pkg : packages) {
            String initializerClassName = pkg.getInitializerClass();
            log.info(String.format("Loading %s", initializerClassName));
            SoftwarePackageInitializer initializer;
            try {
                Class<?> c = Class.forName(initializerClassName);
                initializer = (SoftwarePackageInitializer) c.newInstance();
                context = initializer.initialize(context);
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                log.error(LedpException.buildMessage(LedpCode.LEDP_27004, new String[] { initializerClassName }), e);
            }
        }
        return context;
    }

    public LineMapper<T> getLineMapper() {
        return lineMapper;
    }

    public void setLineMapper(LineMapper<T> lineMapper) {
        this.lineMapper = lineMapper;
    }

    public ItemWriter<String> getItemWriter() {
        return itemWriter;
    }

    public void setItemWriter(ItemWriter<String> itemWriter) {
        this.itemWriter = itemWriter;
    }

    public Class<T> getType() {
        return type;
    }

    public class SingleContainerLineMapper implements LineMapper<T> {

        @Override
        public T mapLine(String line, int lineNumber) throws Exception {
            log.info("Parsing line " + line);
            JSONParser parser = new JSONParser();
            JSONObject jsonObj = (JSONObject) parser.parse(line);
            return JsonUtils.deserialize(jsonObj.toString(), type);
        }

    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        String strAppId = stepExecution.getJobParameters().getString(ContainerRuntimeProperty.APPLICATION_ID.name());
        if (strAppId != null) {
            appId = YarnUtils.appIdFromString(strAppId);
        }
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        return null;
    }

}

