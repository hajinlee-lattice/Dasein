package com.latticeengines.spark.service.impl;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;

import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;
import org.apache.livy.scalaapi.LivyScalaClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.spark.service.LivyClientService;

@Service("livyClientService")
public class LivyClientServiceImpl implements LivyClientService {

    private static final Logger log = LoggerFactory.getLogger(LivyClientServiceImpl.class);

    @Inject
    private VersionManager versionManager;

    @Value("${dataplatform.hdfs.stack}")
    private String stackName;

    public LivyScalaClient createClient(String host, Iterable<String> extraJars) {
        try {
            LivyClient javaClient = new LivyClientBuilder() //
                    .setURI(new URI(host)) //
                    .build();
            String version = versionManager.getCurrentVersion();
            String shadedJar = String.format("/app/%s/%s/spark/lib/le-spark-%s-shaded.jar", stackName, version,
                    version);
            log.info("Adding spark shaded jar to client " + host + ": " + shadedJar);
            javaClient.addJar(new URI("hdfs://" + shadedJar));
            if (extraJars != null) {
                extraJars.forEach(p -> {
                    try {
                        javaClient.addJar(new URI("hdfs://" + p));
                    } catch (URISyntaxException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            Matcher m = Pattern.compile("(.*)/sessions/([0-9]+)").matcher(host);
            boolean sharedContext = m.matches();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> javaClient.stop(!sharedContext)));
            if (sharedContext) { // wait for loading jars
                Thread.sleep(60000L);
            }
            return new LivyScalaClient(javaClient);
        } catch (IOException | URISyntaxException | InterruptedException e) {
            throw new RuntimeException("Failed to create livy client.", e);
        }
    }

}
