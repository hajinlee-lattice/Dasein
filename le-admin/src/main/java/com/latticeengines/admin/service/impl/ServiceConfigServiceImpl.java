package com.latticeengines.admin.service.impl;

import java.util.Map;

import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.service.ServiceConfigService;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component("serviceConfigService")
public class ServiceConfigServiceImpl implements ServiceConfigService {
    private static final Logger log = LoggerFactory.getLogger(ServiceConfigServiceImpl.class);

    private final String SERVICENAME = "CDL";
    private final String INVOKETIME = "/InvokeTime";

    @Override
    public SerializableDocumentDirectory setDefaultInvokeTime(String serviceName, SerializableDocumentDirectory rawDir) {
        log.info(String.format("set default invoke time, service name: %s", serviceName));
        if(serviceName.equals(SERVICENAME)) {
            String value = String.valueOf((Integer.parseInt(getInvokeTimeFromZK()) + 2) % 24);
            Map<String, String> config = rawDir.flatten();
            if (!config.containsKey(INVOKETIME)) {
                config.put(INVOKETIME, value);
            } else {
                if (config.get(INVOKETIME).equals("-1")) {
                    config.replace(INVOKETIME, value);
                }
            }
            return new SerializableDocumentDirectory(config);
        }
        return rawDir;
    }

    @Override
    public void verifyInvokeTime(String serviceName, boolean allowAutoSchedule, String nodePath, String data) {
        log.info(String.format("verify invoke time. service name: %s, node path: %s, data: %s", serviceName, nodePath, data));
        if(allowAutoSchedule && serviceName.equals(SERVICENAME) && nodePath.equals(INVOKETIME)) {
            setInvokeTimeToZK(data);
        }
    }

    @Override
    public void verifyInvokeTime(boolean allowAutoSchedule, Map<String, Map<String, String>> props) {
        log.info("verify invoke time");
        if(allowAutoSchedule && props.containsKey(SERVICENAME) && props.get(SERVICENAME).containsKey(INVOKETIME)) {
            setInvokeTimeToZK(props.get(SERVICENAME).get(INVOKETIME));
        }
    }

    public String getInvokeTimeFromZK() {
        String  invokeTime = "0";
        Path path = PathBuilder.buildInvokeTimePath(CamilleEnvironment.getPodId());
        log.info(String.format("path: %s", path.toString()));
        Camille camille = CamilleEnvironment.getCamille();
        try {
            if (!camille.exists(path)) {
                camille.create(path, new Document(invokeTime), ZooDefs.Ids.OPEN_ACL_UNSAFE);
            } else {
                invokeTime = camille.get(path).getData();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        log.info(String.format("get invoke time form zk, data: %s", invokeTime));
        return invokeTime;
    }

    public void setInvokeTimeToZK(String data) {
        log.info(String.format("set invoke time to zk, data: %s", data));
        int time = Integer.parseInt(data);
        if (time >= 24) {
            throw new LedpException(LedpCode.LEDP_40008);
        }

        Path path = PathBuilder.buildInvokeTimePath(CamilleEnvironment.getPodId());
        Camille camille = CamilleEnvironment.getCamille();
        try {
            if (!camille.exists(path)) {
                camille.create(path, new Document(data), ZooDefs.Ids.OPEN_ACL_UNSAFE);
            } else {
                camille.set(path, new Document(data));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
