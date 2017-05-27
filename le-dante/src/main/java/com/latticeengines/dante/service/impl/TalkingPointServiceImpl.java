package com.latticeengines.dante.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dante.entitymgr.TalkingPointEntityMgr;
import com.latticeengines.dante.service.TalkingPointService;
import com.latticeengines.domain.exposed.dante.DanteTalkingPoint;

@Component("talkingPointService")
public class TalkingPointServiceImpl implements TalkingPointService {

    @Autowired
    private TalkingPointEntityMgr talkingPointEntityMgr;

    public String createOrUpdate(DanteTalkingPoint dtp) {
        talkingPointEntityMgr.createOrUpdate(dtp);
        return talkingPointEntityMgr.findByExternalID(dtp.getExternalID()).getExternalID();
    }

    public DanteTalkingPoint findByExternalID(String externalID) {
        return talkingPointEntityMgr.findByExternalID(externalID);
    }

    public List<DanteTalkingPoint> findAllByPlayID(String playExternalID) {
        return talkingPointEntityMgr.findAllByPlayID(playExternalID);
    }

    public void delete(DanteTalkingPoint dtp) {
        talkingPointEntityMgr.delete(dtp);
    }
}
