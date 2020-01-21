package com.latticeengines.pls.controller;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.auth.exposed.entitymanager.GlobalAuthTicketEntityMgr;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthUserEntityMgr;
import com.latticeengines.domain.exposed.auth.GlobalAuthTicket;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.JwtReplyParameters;
import com.latticeengines.domain.exposed.pls.JwtRequestParameters;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.jwt.JwtManager;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "jwt", description = "REST resource for Jwt single sign on")
@RestController
@RequestMapping("/jwt")
public class JwtResource {

    private static final Logger log = Logger.getLogger(JwtResource.class);

    @Autowired
    private GlobalAuthTicketEntityMgr gaTicketEntityMgr;

    @Autowired
    private GlobalAuthUserEntityMgr gaUserEntityMgr;

    @Autowired
    private JwtManager jwtManager;

    @RequestMapping(value = "/handle_request", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get the jwt redirect URL. for Zendesk handler, 'return_to' and 'source_ref' are required in the post body")
    public JwtReplyParameters getJwtToken(@RequestBody JwtRequestParameters reqParams,
            @RequestHeader(Constants.AUTHORIZATION) String auth) throws LedpException {
        Ticket ticket = new Ticket(auth);
        GlobalAuthTicket ticketData = gaTicketEntityMgr.findByTicket(ticket.getData());
        if (ticketData == null) {
            throw new LedpException(LedpCode.LEDP_18123);
        }
        GlobalAuthUser userData = gaUserEntityMgr.findByUserId(ticketData.getUserId());
        if (userData == null) {
            throw new LedpException(LedpCode.LEDP_10004, new String[] { "USER" });
        }
        JwtReplyParameters reply = jwtManager.handleJwtRequest(userData, reqParams);
        log.info(reply);
        return reply;
    }
}
