package com.latticeengines.dante.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dante.entitymgr.DanteTalkingPointEntityMgr;
import com.latticeengines.dante.entitymgr.TalkingPointEntityMgr;
import com.latticeengines.dante.service.TalkingPointService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dante.DantePreviewResources;
import com.latticeengines.domain.exposed.dante.DanteTalkingPoint;
import com.latticeengines.domain.exposed.dante.DanteTalkingPointValue;
import com.latticeengines.domain.exposed.dante.TalkingPointPreview;
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
    private String danteUrl;

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

    void setInternalResourceRestApiProxy(InternalResourceRestApiProxy internalResourceRestApiProxy) {
        this.internalResourceRestApiProxy = internalResourceRestApiProxy;
    }

    @PostConstruct
    public void initialize() throws Exception {
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
    }

    @Override
    public List<TalkingPointDTO> createOrUpdate(List<TalkingPointDTO> tps, String customerSpace) {
        if (tps == null || tps.size() < 1) {
            log.info("Attempted to update or create empty set of talking points");
            return new ArrayList<>();
        }

        if (tps.stream().anyMatch(tp -> tp.getPlayName() == null || tp.getPlayName().isEmpty())) {
            throw new LedpException(LedpCode.LEDP_38018);
        }

        if (tps.stream().anyMatch(x -> !x.getPlayName().equals(tps.get(0).getPlayName()))) {
            throw new LedpException(LedpCode.LEDP_38011);
        }

        Play play;
        try {
            play = internalResourceRestApiProxy.findPlayByName(CustomerSpace.parse(customerSpace),
                    tps.get(0).getPlayName());
            if (play == null) {
                throw new LedpException(LedpCode.LEDP_38012, new String[] { tps.get(0).getPlayName() });
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_38013, e);
        }

        try {
            for (TalkingPointDTO tpdto : tps) {
                if (tpdto.getPid() != null && tpdto.getPid() == 0)
                    tpdto.setPid(null);
                TalkingPoint tp = tpdto.convertToTalkingPoint(play);
                talkingPointEntityMgr.createOrUpdate(tp);
            }
            return findAllByPlayName(play.getName());
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new LedpException(LedpCode.LEDP_38002);
        }
    }

    @Override
    public TalkingPointDTO findByName(String name) {
        TalkingPoint tp = talkingPointEntityMgr.findByField("name", name);
        if (tp != null)
            return new TalkingPointDTO(tp);
        else
            throw new LedpException(LedpCode.LEDP_38001, new String[] { name });
    }

    public List<TalkingPointDTO> findAllByPlayName(String playName) {
        try {
            return talkingPointEntityMgr.findAllByPlayName(playName).stream().map(TalkingPointDTO::new)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_38016, e, new String[] { playName });
        }
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

    @Override
    public void delete(String name) {
        try {
            TalkingPoint tp = talkingPointEntityMgr.findByName(name);
            if (tp == null) {
                throw new LedpException(LedpCode.LEDP_38001, new String[] { name });
            }
            talkingPointEntityMgr.delete(tp);
        } catch (LedpException e) {
            throw e;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_38017, e, new String[] { name });
        }
    }

    @Override
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
                        .createOrUpdate(convertForDante(tp, CustomerSpace.parse(customerSpace).getTenantId()));
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_38014, e, new String[] { playName, customerSpace });
        }
    }

    @Override
    public TalkingPointPreview getPreview(String playName, String customerSpace) {
        try {
            List<DanteTalkingPointValue> dtps = talkingPointEntityMgr.findAllByPlayName(playName).stream()
                    .sorted((tp1, tp2) -> Integer.compare(tp1.getOffset(), tp2.getOffset()))
                    .map(DanteTalkingPointValue::new).collect(Collectors.toList());
            return new TalkingPointPreview(dtps);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_38015, e, new String[] { playName, customerSpace });
        }
    }

    @Override
    public List<TalkingPointDTO> revertToLastPublished(String playName, String customerSpace) {
        try {
            for (TalkingPoint tp : talkingPointEntityMgr.findAllByPlayName(playName)) {
                talkingPointEntityMgr.delete(tp);
            }

            Play play = internalResourceRestApiProxy.findPlayByName(CustomerSpace.parse(customerSpace), playName);

            for (DanteTalkingPoint dtp : danteTalkingPointEntityMgr.findAllByPlayID(playName)) {
                talkingPointEntityMgr.create(convertFromDante(dtp, play));
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_38019, e, new String[] { playName, customerSpace });
        }

        return findAllByPlayName(playName);
    }

    private TalkingPoint convertFromDante(DanteTalkingPoint dtp, Play play) {
        TalkingPoint tp = new TalkingPoint();
        tp.setPlay(play);
        // set created first to ensure a new unique name is not generated
        tp.setCreated(dtp.getCreationDate());
        tp.setName(dtp.getExternalID());
        tp.setUpdated(dtp.getLastModificationDate());
        DanteTalkingPointValue dtpv = JsonUtils.deserialize(dtp.getValue(), DanteTalkingPointValue.class);
        tp.setTitle(dtpv.getTitle());
        tp.setContent(dtpv.getContent());
        tp.setOffset(dtpv.getOffset());

        return tp;
    }

    private DanteTalkingPoint convertForDante(TalkingPoint tp, String customerId) {
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
