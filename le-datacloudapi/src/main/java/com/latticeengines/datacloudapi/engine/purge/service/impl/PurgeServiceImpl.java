package com.latticeengines.datacloudapi.engine.purge.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloudapi.engine.purge.service.PurgeService;
import com.latticeengines.datacloudapi.engine.purge.service.SourcePurger;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeSource;

@Component("purgeService")
public class PurgeServiceImpl implements PurgeService {

    @Autowired
    private List<SourcePurger> sourcePurgers;

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

}
