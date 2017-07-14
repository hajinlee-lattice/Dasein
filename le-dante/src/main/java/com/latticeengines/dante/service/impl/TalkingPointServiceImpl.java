package com.latticeengines.dante.service.impl;

import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.dante.entitymgr.DanteTalkingPointEntityMgr;
import com.latticeengines.dante.entitymgr.TalkingPointEntityMgr;
import com.latticeengines.dante.metadata.DanteTalkingPointValue;
import com.latticeengines.dante.service.TalkingPointService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dante.DantePreviewResources;
import com.latticeengines.domain.exposed.dante.DanteTalkingPoint;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.oauth.OauthClientType;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.TalkingPoint;
import com.latticeengines.domain.exposed.pls.TalkingPointDTO;
import com.latticeengines.proxy.exposed.oauth2.Oauth2RestApiProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;

@Component("talkingPointService")
public class TalkingPointServiceImpl implements TalkingPointService {
    private static final Logger log = LoggerFactory.getLogger(TalkingPointServiceImpl.class);

    @Value("${common.dante.url}")
    private String danteUrl; // TODO: correct for envs

    @Value("${common.playmaker.url}")
    private String playmakerApiUrl;

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private final String oAuth2DanteAppId = "lattice.web.dante";

    @Autowired
    private Oauth2RestApiProxy oauth2RestApiProxy;

    @Autowired
    private DanteTalkingPointEntityMgr danteTalkingPointEntityMgr;

    @Autowired
    private TalkingPointEntityMgr talkingPointEntityMgr;

    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    @PostConstruct
    public void initialize() throws Exception {
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
    }

    public String createOrUpdate(List<TalkingPointDTO> tps, String customerSpace) {
        if (tps == null || tps.size() < 1)
            log.info("No Talking points created or updated");

        if (tps.stream().anyMatch(x -> !x.getPlayName().equals(tps.get(0).getPlayName()))) {
            throw new LedpException(LedpCode.LEDP_38011);
        }

        Play play;
        try {
            play = internalResourceRestApiProxy.findPlayByName(tps.get(0).getPlayName(), customerSpace);
            if (play == null) {
                throw new LedpException(LedpCode.LEDP_38012, new String[] { tps.get(0).getPlayName() });
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_38013, e);
        }

        try {
            for (TalkingPointDTO tpdto : tps) {
                talkingPointEntityMgr.createOrUpdate(tpdto.convertToTalkingPoint(play));
            }
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new LedpException(LedpCode.LEDP_38002);
        }
        return "Success";
    }

    public TalkingPointDTO findByName(String name) {
        TalkingPoint tp = talkingPointEntityMgr.findByField("name", name);
        if (tp != null)
            return new TalkingPointDTO(tp);
        else
            throw new LedpException(LedpCode.LEDP_38001, new String[] { name });
    }

    public List<TalkingPointDTO> findAllByPlayName(String playName) {
        return talkingPointEntityMgr.findAllByPlayName(playName).stream().map(TalkingPointDTO::new)
                .collect(Collectors.toList());
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

    public void delete(String name) {
        TalkingPoint tp = talkingPointEntityMgr.findByName(name);
        if (tp == null) {
            throw new LedpException(LedpCode.LEDP_38001, new String[] { name });
        }
        talkingPointEntityMgr.delete(tp);
    }

    public void publish(String playName, String customerSpace) {
        try {
            log.info("Publishing Talkingpoints for play " + playName + " to Dante");
            List<TalkingPoint> tps = talkingPointEntityMgr.findAllByPlayName(playName);
            List<DanteTalkingPoint> toBeDeleted = danteTalkingPointEntityMgr.findAllByPlayID(playName);

            for (DanteTalkingPoint dtp : toBeDeleted) {
                danteTalkingPointEntityMgr.delete(dtp);
            }

            for (TalkingPoint tp : tps) {
                danteTalkingPointEntityMgr
                        .createOrUpdate(covertForDante(tp, CustomerSpace.parse(customerSpace).getTenantId()));
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_38014, e, new String[] { playName, customerSpace });
        }
    }

    private DanteTalkingPoint covertForDante(TalkingPoint tp, String customerId) {
        DanteTalkingPoint dtp = new DanteTalkingPoint();
        dtp.setCreationDate(tp.getCreated());
        dtp.setCustomerID(customerId);
        dtp.setExternalID(tp.getName());
        dtp.setLastModificationDate(tp.getUpdated());
        dtp.setPlayExternalID(tp.getPlay().getName());
        dtp.setValue(new DanteTalkingPointValue(tp).toString());
        return dtp;
    }
}
