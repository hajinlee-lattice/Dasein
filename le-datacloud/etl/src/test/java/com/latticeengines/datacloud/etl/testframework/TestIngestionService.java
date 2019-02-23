package com.latticeengines.datacloud.etl.testframework;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.tuple.Triple;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.etl.ingestion.entitymgr.IngestionEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion.IngestionType;

@Component("testIngestionService")
public class TestIngestionService {

    @Inject
    private IngestionEntityMgr ingestionEntityMgr;

    /**
     * @param ingestionInfo:
     *            Triple<IngestionName, IngestionConfig, IngestionType>
     * @return
     */
    public List<Ingestion> createIngestions(List<Triple<String, String, IngestionType>> ingestionInfo) {
        List<Ingestion> ingestions = new ArrayList<>();
        ingestionInfo.forEach(info -> {
            Ingestion existing = ingestionEntityMgr.getIngestionByName(info.getLeft());
            if (existing != null) {
                ingestionEntityMgr.delete(existing);
            }
            Ingestion ingestion = new Ingestion();
            ingestion.setIngestionName(info.getLeft());
            ingestion.setConfig(info.getMiddle());
            ingestion.setSchedularEnabled(Boolean.TRUE);
            ingestion.setNewJobRetryInterval(10000L);
            ingestion.setNewJobMaxRetry(1);
            ingestion.setIngestionType(info.getRight());
            ingestionEntityMgr.save(ingestion);
            ingestions.add(ingestionEntityMgr.getIngestionByName(ingestion.getIngestionName()));
        });
        return ingestions;
    }

}
