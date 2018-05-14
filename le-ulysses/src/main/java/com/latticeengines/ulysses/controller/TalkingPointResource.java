package com.latticeengines.ulysses.controller;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.multitenant.TalkingPointDTO;
import com.latticeengines.domain.exposed.ulysses.FrontEndResponse;
import com.latticeengines.proxy.exposed.dante.TalkingPointProxy;
import com.latticeengines.ulysses.utils.DanteFormatter;
import com.latticeengines.ulysses.utils.TalkingPointDanteFormatter;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "TalkingPoints", description = "Common REST resource to lookup talking points")
@RestController
@RequestMapping("/talkingpoints")
public class TalkingPointResource {
    private static final Logger log = LoggerFactory.getLogger(TalkingPointResource.class);

    @Inject
    private TalkingPointProxy talkingPointProxy;

    @Inject
    @Qualifier(TalkingPointDanteFormatter.Qualifier)
    private DanteFormatter<TalkingPointDTO> talkingPointDanteFormatter;

    @RequestMapping(value = "/{talkingPointId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get an account by of attributes in a group")
    public TalkingPointDTO getTalkingPointById(@PathVariable String talkingPointId) {
        return talkingPointProxy.findByName(talkingPointId);
    }

    @RequestMapping(value = "/playid/{playId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get published talking points for the given play")
    public List<TalkingPointDTO> getTalkingPointByPlayId(@PathVariable String playId) {
        return talkingPointProxy.findAllByPlayName(playId, true);
    }

    @RequestMapping(value = "/{talkingPointId}/danteformat", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get an account by of attributes in a group")
    public FrontEndResponse<String> getTalkingPointByIdInDanteFormat(@PathVariable String talkingPointId) {
        try {
            return new FrontEndResponse<>(talkingPointDanteFormatter.format(getTalkingPointById(talkingPointId)));
        } catch (LedpException le) {
            log.error("Failed to get talking point data", le);
            return new FrontEndResponse<>(le.getErrorDetails());
        } catch (Exception e) {
            log.error("Failed to get talking point data", e);
            return new FrontEndResponse<>(new LedpException(LedpCode.LEDP_00002, e).getErrorDetails());
        }
    }

    @RequestMapping(value = "/playid/{playId}/danteformat", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get published talking points for the given play")
    public FrontEndResponse<List<String>> getTalkingPointByPlayIdInDanteFormat(@PathVariable String playId) {
        try {
            return new FrontEndResponse<>(talkingPointDanteFormatter
                    .format(JsonUtils.convertList(getTalkingPointByPlayId(playId), TalkingPointDTO.class)));
        } catch (LedpException le) {
            log.error("Failed to get talking point data", le);
            return new FrontEndResponse<>(le.getErrorDetails());
        } catch (Exception e) {
            log.error("Failed to get talking point data", e);
            return new FrontEndResponse<>(new LedpException(LedpCode.LEDP_00002, e).getErrorDetails());
        }
    }
}
