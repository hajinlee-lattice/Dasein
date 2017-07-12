package com.latticeengines.dante.service.impl;

import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.dante.entitymgr.DanteTalkingPointEntityMgr;
import com.latticeengines.dante.entitymgr.TalkingPointEntityMgr;
import com.latticeengines.dante.service.TalkingPointService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dante.DantePreviewResources;
import com.latticeengines.domain.exposed.dante.DanteTalkingPoint;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.oauth.OauthClientType;
import com.latticeengines.domain.exposed.pls.TalkingPoint;
import com.latticeengines.proxy.exposed.oauth2.Oauth2RestApiProxy;

@Component("talkingPointService")
public class TalkingPointServiceImpl implements TalkingPointService {
    private static final Logger log = Logger.getLogger(TalkingPointServiceImpl.class);

    @Value("${common.dante.url}")
    private String danteUrl; // TODO: correct for envs

    @Value("${common.playmaker.url}")
    private String playmakerApiUrl;

    private final String oAuth2DanteAppId = "lattice.web.dante";

    @Autowired
    private Oauth2RestApiProxy oauth2RestApiProxy;

    @Autowired
    private DanteTalkingPointEntityMgr danteTalkingPointEntityMgr;

    @Autowired
    private TalkingPointEntityMgr talkingPointEntityMgr;

    public String createOrUpdate(List<TalkingPoint> tps) {
        try {
            for (TalkingPoint tp : tps) {
                talkingPointEntityMgr.createOrUpdate(tp);
            }
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new LedpException(LedpCode.LEDP_38002);
        }
        return "Success";
    }

    public TalkingPoint findByName(String name) {
        TalkingPoint tp = talkingPointEntityMgr.findByField("name", name);
        if (tp != null)
            return tp;
        else
            throw new LedpException(LedpCode.LEDP_38001, new String[] { name });
    }

    public List<TalkingPoint> findAllByPlayId(Long playId) {
        return talkingPointEntityMgr.findAllByPlayID(playId);
    }

    @Override
    public DantePreviewResources getPreviewResources(String customerSpace) {
        try {
            String token = oauth2RestApiProxy.createOAuth2AccessToken(CustomerSpace.parse(customerSpace).toString(),
                    oAuth2DanteAppId, OauthClientType.PLAYMAKER).getValue();
            return new DantePreviewResources(danteUrl, playmakerApiUrl, token);
        } catch (LedpException e) {
            throw e;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_38010, e);
        }
    }

    public void delete(TalkingPoint tp) {
        talkingPointEntityMgr.delete(tp);
    }

    public void publish(Long playId) {
        List<TalkingPoint> tps = findAllByPlayId(playId);
        throw new NotImplementedException();
        // for (TalkingPoint tp : tps) {
        // danteTalkingPointEntityMgr.createOrUpdate(covertForDante(tp));
        // }
    }

    private DanteTalkingPoint covertForDante(TalkingPoint tp) {
        return null;
    }
}
