package com.latticeengines.propdata.collection.service.impl;

import javax.annotation.PostConstruct;

import org.apache.zookeeper.ZooDefs;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.propdata.collection.service.ZkConfigurationService;
import com.latticeengines.propdata.collection.source.impl.CollectionSource;
import com.latticeengines.propdata.collection.source.impl.PivotedSource;
import com.latticeengines.propdata.collection.source.Source;

@Component
public class ZkConfigurationServiceImpl implements ZkConfigurationService {

    private Camille camille;
    private String podId;
    private static final String PROPDATA_SERVICE = "PropData";
    private static final String SOURCES = "Sources";
    private static final String JOB_ENABLED = "JobEnabled";

    @PostConstruct
    private void postConstruct() throws Exception {
        camille = CamilleEnvironment.getCamille();
        podId = CamilleEnvironment.getPodId();

        for (CollectionSource source: CollectionSource.values()) {
            Path flagPath = jobFlagPath(source);
            if (!camille.exists(flagPath)) {
                camille.create(flagPath, ZooDefs.Ids.OPEN_ACL_UNSAFE);
            }
        }

        for (PivotedSource source: PivotedSource.values()) {
            Path flagPath = jobFlagPath(source);
            if (!camille.exists(flagPath)) {
                camille.create(flagPath, ZooDefs.Ids.OPEN_ACL_UNSAFE);
            }
        }

    }

    @Override
    public boolean refreshJobEnabled(Source source) {
        Path flagPath = jobFlagPath(source);
        try {
            if (camille.exists(flagPath)) {
                return Boolean.valueOf(camille.get(flagPath).getData());
            } else {
                camille.create(flagPath, ZooDefs.Ids.OPEN_ACL_UNSAFE);
                return false;
            }
        } catch (Exception e) {
            return false;
        }
    }


    private Path jobFlagPath(Source source) {
        Path propDataPath = PathBuilder.buildServicePath(podId, PROPDATA_SERVICE);
        Path sourcePath = propDataPath.append(SOURCES).append(source.getSourceName());
        return sourcePath.append(JOB_ENABLED);
    }

}
