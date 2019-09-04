package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.apps.cdl.entitymgr.PublishedTalkingPointEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.TalkingPointEntityMgr;
import com.latticeengines.apps.cdl.service.PlayService;
import com.latticeengines.apps.cdl.service.TalkingPointService;
import com.latticeengines.apps.cdl.util.OAuthAccessTokenCache;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.DantePreviewResources;
import com.latticeengines.domain.exposed.cdl.DanteTalkingPointValue;
import com.latticeengines.domain.exposed.cdl.PublishedTalkingPoint;
import com.latticeengines.domain.exposed.cdl.TalkingPoint;
import com.latticeengines.domain.exposed.cdl.TalkingPointDTO;
import com.latticeengines.domain.exposed.cdl.TalkingPointPreview;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("talkingPointService")
public class TalkingPointServiceImpl implements TalkingPointService {
    private static final Logger log = LoggerFactory.getLogger(TalkingPointServiceImpl.class);

    @Value("${common.dante.url}")
    private String danteUrl;

    @Value("${common.ulysses.url}")
    private String ulyssesUrl;

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    @Inject
    private PlayService playService;

    @Inject
    private PublishedTalkingPointEntityMgr publishedTalkingPointEntityMgr;

    @Inject
    private TalkingPointEntityMgr talkingPointEntityMgr;

    @Inject
    private OAuthAccessTokenCache oAuthAccessTokenCache;

    @VisibleForTesting
    void setPlayService(PlayService playService) {
        this.playService = playService;
    }

    @Override
    public List<TalkingPointDTO> createOrUpdate(List<TalkingPointDTO> tps) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
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
            play = playService.getFullPlayByName(tps.get(0).getPlayName(), false);
            if (play == null) {
                throw new LedpException(LedpCode.LEDP_38012, new String[]{tps.get(0).getPlayName()});
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
        return findAllByPlayName(playName, false);
    }

    public List<TalkingPointDTO> findAllByPlayName(String playName, boolean publishedOnly) {
        try {
            return publishedOnly //
                    ? publishedTalkingPointEntityMgr.findAllByPlayName(playName).stream().map(TalkingPointDTO::new)
                            .collect(Collectors.toList()) //
                    : talkingPointEntityMgr.findAllByPlayName(playName).stream().map(TalkingPointDTO::new)
                            .collect(Collectors.toList());
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_38016, e, new String[] { playName });
        }
    }

    @Override
    public DantePreviewResources getPreviewResources() {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        try {
            String token = oAuthAccessTokenCache.getOauthTokenFromCache(customerSpace);

            if (oAuthAccessTokenCache.isInvalidToken(customerSpace, token)) {
                token = oAuthAccessTokenCache.refreshOauthTokenInCache(customerSpace);
            }
            return new DantePreviewResources(danteUrl, ulyssesUrl, token);
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
    public void publish(String playName) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        try {
            log.info("Publishing Talkingpoints for play " + playName + " for availabilty in BIS");
            List<TalkingPoint> tps = talkingPointEntityMgr.findAllByPlayName(playName);
            List<PublishedTalkingPoint> toBeDeleted = publishedTalkingPointEntityMgr.findAllByPlayName(playName);

            for (PublishedTalkingPoint ptp : toBeDeleted) {
                publishedTalkingPointEntityMgr.delete(ptp);
            }

            for (TalkingPoint tp : tps) {
                publishedTalkingPointEntityMgr.createOrUpdate(convertToPublished(tp));
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_38014, e, new String[] { playName, customerSpace });
        }
    }

    @Override
    public TalkingPointPreview getPreview(String playName) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        try {
            List<DanteTalkingPointValue> dtps = talkingPointEntityMgr.findAllByPlayName(playName).stream()
                    .sorted(Comparator.comparingInt(TalkingPoint::getOffset)).map(DanteTalkingPointValue::new)
                    .collect(Collectors.toList());
            return new TalkingPointPreview(dtps);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_38015, e, new String[] { playName, customerSpace });
        }
    }

    @Override
    public List<TalkingPointDTO> revertToLastPublished(String playName) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        try {
            for (TalkingPoint tp : talkingPointEntityMgr.findAllByPlayName(playName)) {
                talkingPointEntityMgr.delete(tp);
            }

            Play play = playService.getFullPlayByName(playName, false);

            for (PublishedTalkingPoint ptp : publishedTalkingPointEntityMgr.findAllByPlayName(playName)) {
                talkingPointEntityMgr.create(convertFromPublished(ptp, play));
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_38019, e, new String[] { playName, customerSpace });
        }

        return findAllByPlayName(playName);
    }

    public List<TalkingPointDTO> findAllPublishedByTenant(String customerSpace) {
        Tenant tenant = MultiTenantContext.getTenant();
        if (tenant != null && tenant.getPid() != null && tenant.getPid() > 0) {
            return publishedTalkingPointEntityMgr.findAllByTenantPid(tenant.getPid());
        }
        throw new LedpException(LedpCode.LEDP_38009, new String[] { customerSpace });
    }

    @Override
    public List<AttributeLookup> getAttributesInTalkingPointOfPlay(String playName) {
        Set<AttributeLookup> attributes = new HashSet<>();
        List<TalkingPoint> talkingPoints = talkingPointEntityMgr.findAllByPlayName(playName);
        if (talkingPoints != null) {
            for (TalkingPoint talkingPoint : talkingPoints) {
                if (StringUtils.isNotBlank(talkingPoint.getContent())) {
                    talkingPoint.setTPAttributes(findAttributeLookups(talkingPoint.getContent()));
                    attributes.addAll(talkingPoint.getTPAttributes());
                }
            }
        }

        return new ArrayList<>(attributes);
    }

    private Set<AttributeLookup> findAttributeLookups(String content) {
        Pattern pattern = Pattern.compile("\\{!([A-Za-z0-9_]+)\\.([A-Za-z0-9_]+)}");
        Matcher matcher = pattern.matcher(content);

        Set<AttributeLookup> attributes = new HashSet<>();
        while (matcher.find()) {
            String attributeLookupStr = String.format("%s.%s", matcher.group(1), matcher.group(2));
            AttributeLookup attributeLookup = AttributeLookup.fromString(attributeLookupStr);
            if (attributeLookup != null) {
                attributes.add(attributeLookup);
            }
        }

        return attributes;
    }

    private TalkingPoint convertFromPublished(PublishedTalkingPoint ptp, Play play) {
        TalkingPoint tp = new TalkingPoint();
        tp.setPlay(play);
        // set created first to ensure a new unique name is not generated
        tp.setCreated(ptp.getCreated());
        tp.setName(ptp.getName());
        tp.setUpdated(ptp.getUpdated());
        tp.setTitle(ptp.getTitle());
        tp.setContent(ptp.getContent());
        tp.setOffset(ptp.getOffset());
        tp.setPlay(play);
        return tp;
    }

    private PublishedTalkingPoint convertToPublished(TalkingPoint tp) {
        PublishedTalkingPoint ptp = new PublishedTalkingPoint();
        ptp.setCreated(tp.getCreated());
        ptp.setName(tp.getName());
        ptp.setUpdated(tp.getUpdated());
        ptp.setPlayName(tp.getPlay().getName());
        ptp.setTitle(tp.getTitle());
        ptp.setContent(tp.getContent());
        ptp.setOffset(tp.getOffset());
        return ptp;
    }
}
