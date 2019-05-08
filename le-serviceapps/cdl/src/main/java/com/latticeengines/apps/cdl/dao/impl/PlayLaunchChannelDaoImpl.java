package com.latticeengines.apps.cdl.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.PlayLaunchChannelDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;

@Component("playLaunchChannelDao")
public class PlayLaunchChannelDaoImpl extends BaseDaoImpl<PlayLaunchChannel> implements PlayLaunchChannelDao {
    @Override
    protected Class<PlayLaunchChannel> getEntityClass() {
        return PlayLaunchChannel.class;
    }
}
