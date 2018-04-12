package com.latticeengines.datacloudapi.engine.purge.service.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.etl.purge.entitymgr.PurgeStrategyEntityMgr;
import com.latticeengines.datacloudapi.engine.purge.service.PurgeService;
import com.latticeengines.datacloudapi.engine.purge.service.SourcePurger;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeSource;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy.SourceType;

@Component("purgeService")
public class PurgeServiceImpl implements PurgeService {

    private static Logger log = LoggerFactory.getLogger(PurgeServiceImpl.class);

    @Autowired
    private List<SourcePurger> sourcePurgers;

    @Autowired
    private PurgeStrategyEntityMgr purgeStrategyEntityMgr;

    @Autowired
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Override
    public List<PurgeSource> scan(String hdfsPod, boolean debug) {
        if (StringUtils.isEmpty(hdfsPod)) {
            hdfsPod = HdfsPodContext.getDefaultHdfsPodId();
            HdfsPodContext.changeHdfsPodId(hdfsPod);
        }

        List<PurgeSource> toPurge = new ArrayList<>();
        sourcePurgers.forEach(srcPurger -> {
            List<PurgeSource> list = srcPurger.findSourcesToPurge(debug);
            if (CollectionUtils.isNotEmpty(list)) {
                toPurge.addAll(list);
            }
        });
        return toPurge;
    }

    @Override
    public List<String> scanUnknownSources(String hdfsPod) {
        if (StringUtils.isEmpty(hdfsPod)) {
            hdfsPod = HdfsPodContext.getDefaultHdfsPodId();
            HdfsPodContext.changeHdfsPodId(hdfsPod);
        }

        List<PurgeStrategy> strategies = purgeStrategyEntityMgr.findAll();
        Set<String> generalSources = new HashSet<>();
        List<String> tempSources = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(strategies)) {
            strategies.forEach(strategy -> {
                if (strategy.getSourceType() == SourceType.TEMP_SOURCE) {
                    tempSources.add(strategy.getSource());
                } else {
                    generalSources.add(strategy.getSource());
                }
            });
        }

        List<String> sources = new ArrayList<>();
        try {
            sources = hdfsSourceEntityMgr.getAllSources();
            if (CollectionUtils.isNotEmpty(sources)) {
                Iterator<String> it = sources.iterator();
                while (it.hasNext()) {
                    String source = it.next();
                    if (generalSources.contains(source)) {
                        it.remove();
                    }
                    tempSources.forEach(tempSource -> {
                        if (source.startsWith(tempSource)) {
                            it.remove();
                        }
                    });
                }
            }
        } catch (Exception ex) {
            log.error("Fail to scan DataCloud sources");
        }
        return sources;
    }

}
