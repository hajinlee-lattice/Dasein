package com.latticeengines.dante.service.impl;

import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dante.entitymgr.TalkingPointEntityMgr;
import com.latticeengines.dante.service.TalkingPointService;
import com.latticeengines.domain.exposed.dante.DanteTalkingPoint;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component("talkingPointService")
public class TalkingPointServiceImpl implements TalkingPointService {
    private static final Logger log = Logger.getLogger(TalkingPointServiceImpl.class);

    @Autowired
    private TalkingPointEntityMgr talkingPointEntityMgr;

    public String createOrUpdate(DanteTalkingPoint dtp) {
        talkingPointEntityMgr.createOrUpdate(dtp);
        return talkingPointEntityMgr.findByExternalID(dtp.getExternalID()).getExternalID();
    }

    public DanteTalkingPoint findByExternalID(String externalID) {
        DanteTalkingPoint tp = talkingPointEntityMgr.findByExternalID(externalID);
        if (tp != null)
            return tp;
        else
            throw new LedpException(LedpCode.LEDP_38001, new String[] { externalID });
    }

    public List<DanteTalkingPoint> findAllByPlayID(String playExternalID) {
        List<DanteTalkingPoint> tps = talkingPointEntityMgr.findAllByPlayID(playExternalID);
        if (tps != null && tps.size() > 0)
            return tps;
        else
            throw new LedpException(LedpCode.LEDP_38002, new String[] { playExternalID });
    }

    public void delete(DanteTalkingPoint dtp) {
        talkingPointEntityMgr.delete(dtp);
    }
}
