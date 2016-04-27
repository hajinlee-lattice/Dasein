package com.latticeengines.dataplatform.exposed.yarn.runtime;

import java.lang.reflect.ParameterizedType;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.integration.channel.AbstractPollableChannel;
import org.springframework.integration.channel.AbstractSubscribableChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.dispatcher.MessageDispatcher;
import org.springframework.integration.dispatcher.UnicastingDispatcher;
import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.integration.ip.tcp.TcpOutboundGateway;
import org.springframework.integration.ip.tcp.connection.AbstractClientConnectionFactory;
import org.springframework.integration.ip.tcp.connection.TcpNetClientConnectionFactory;
import org.springframework.yarn.integration.ip.mind.DefaultMindAppmasterServiceClient;
import org.springframework.yarn.integration.ip.mind.MindAppmasterServiceClient;
import org.springframework.yarn.integration.ip.mind.MindRpcSerializer;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.SSLUtils;
import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.dataplatform.exposed.yarn.runtime.progress.LedpProgressReporter;
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

    //@Autowired
    protected LedpAppmasterService ledpAppmasterService;

    @Value("${dataplatform.hdfs.stack:}")
    private String stackName;

    @Autowired
    private LedpProgressReporter ledpProgressReporter;

    protected MindAppmasterServiceClient appmasterServiceClient;

    @SuppressWarnings("unchecked")
    public SingleContainerYarnProcessor() {
        this.type = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
        SSLUtils.turnOffSslChecking();
    }

    public ApplicationContext loadSoftwarePackages(String module, SoftwareLibraryService softwareLibraryService,
            ApplicationContext context, VersionManager versionManager) {
        if (softwareLibraryService != null) {
            softwareLibraryService.setStackName(stackName);
        }
        List<SoftwarePackage> packages = softwareLibraryService.getInstalledPackagesByVersion(module, versionManager.getCurrentVersion());
        if (StringUtils.isEmpty(versionManager.getCurrentVersion())) {
            packages = softwareLibraryService.getLatestInstalledPackages(module);
        }
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
            appId = ConverterUtils.toApplicationId(strAppId);
        }
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        return null;
    }

    protected void setProgress(float progress) {
        ledpProgressReporter.setProgress(progress);
    }

    protected void initAppmasterServiceClient(BeanFactory applicationContext) {
        appmasterServiceClient = new DefaultMindAppmasterServiceClient();
        AbstractClientConnectionFactory clientConnectionFactory = new TcpNetClientConnectionFactory("localhost",
                ledpAppmasterService.getPort());
        clientConnectionFactory.setSerializer(new MindRpcSerializer());
        clientConnectionFactory.setDeserializer(new MindRpcSerializer());
        clientConnectionFactory.start();
        TcpOutboundGateway tcpOutboundGateway = new TcpOutboundGateway();
        tcpOutboundGateway.setConnectionFactory(clientConnectionFactory);

        final MessageDispatcher dispatcher = new UnicastingDispatcher();
        dispatcher.addHandler((AbstractMessageHandler) tcpOutboundGateway);
        AbstractSubscribableChannel requestChannel = new AbstractSubscribableChannel() {

            @Override
            protected MessageDispatcher getDispatcher() {
                return dispatcher;
            }
        };

        ((MindAppmasterServiceClient) appmasterServiceClient).setRequestChannel(requestChannel);

        AbstractPollableChannel replyChannel = new QueueChannel();
        tcpOutboundGateway.setReplyChannel(replyChannel);
        ((MindAppmasterServiceClient) appmasterServiceClient).setResponseChannel(replyChannel);
        ((MindAppmasterServiceClient) appmasterServiceClient).setBeanFactory(applicationContext);

    }

}
