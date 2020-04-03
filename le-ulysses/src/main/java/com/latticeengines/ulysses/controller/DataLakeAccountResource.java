package com.latticeengines.ulysses.controller;

import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.CompanyProfile;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.TalkingPoint;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.inject.Inject;
import javax.inject.Provider;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.RequestEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.app.exposed.service.DataLakeService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.TalkingPointDTO;
import com.latticeengines.domain.exposed.exception.ErrorDetails;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.ulysses.FrontEndResponse;
import com.latticeengines.monitor.exposed.annotation.InvocationMeter;
import com.latticeengines.monitor.exposed.metrics.impl.InstrumentRegistry;
import com.latticeengines.proxy.exposed.cdl.TalkingPointProxy;
import com.latticeengines.proxy.exposed.oauth2.Oauth2RestApiProxy;
import com.latticeengines.proxy.exposed.objectapi.PeriodTransactionProxy;
import com.latticeengines.ulysses.utils.AccountDanteFormatter;
import com.latticeengines.ulysses.utils.DanteFormatter;
import com.latticeengines.ulysses.utils.TalkingPointDanteFormatter;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "CDLAccounts", description = "Common REST resource to lookup CDL accounts")
@RestController
@RequestMapping("/datacollection/accounts")
public class DataLakeAccountResource {
    private static final Logger log = LoggerFactory.getLogger(DataLakeAccountResource.class);

    private static final String INSTRUMENT_TP = "TalkingPoint";
    private static final String INSTRUMENT_CP = "CompanyProfile";
    private static final String INSTRUMENT_CL = "CustomList";

    private final DataLakeService dataLakeService;
    private final Oauth2RestApiProxy tenantProxy;
    private final PeriodTransactionProxy periodTransactionProxy;
    private final TalkingPointProxy talkingPointProxy;
    private final BatonService batonService;

    @Inject
    @Qualifier(AccountDanteFormatter.Qualifier)
    private Provider<AccountDanteFormatter> accountDanteFormatterProvider;

    @Resource(name = TalkingPointDanteFormatter.Qualifier)
    private DanteFormatter<TalkingPointDTO> talkingPointDanteFormatter;

    @Inject
    public DataLakeAccountResource(DataLakeService dataLakeService, Oauth2RestApiProxy tenantProxy,
            PeriodTransactionProxy periodTransactionProxy, BatonService batonService,
            TalkingPointProxy talkingPointProxy) {
        this.dataLakeService = dataLakeService;
        this.tenantProxy = tenantProxy;
        this.periodTransactionProxy = periodTransactionProxy;
        this.batonService = batonService;
        this.talkingPointProxy = talkingPointProxy;
    }

    @PostConstruct
    public void postConstruct() {
        InstrumentRegistry.register(INSTRUMENT_TP, new UlyssesInstrument(TalkingPoint));
        InstrumentRegistry.register(INSTRUMENT_CP, new UlyssesInstrument(CompanyProfile));
        InstrumentRegistry.register(INSTRUMENT_CL, new UlyssesInstrument(true));
    }

    @GetMapping(value = "/{accountId}/{attributeGroup}")
    @ResponseBody
    @ApiOperation(value = "Get account with attributes of the attribute group by its Id ")
    @InvocationMeter(name = "talkingpoint", measurment = "ulysses", instrument = INSTRUMENT_TP)
    @InvocationMeter(name = "companyprofile", measurment = "ulysses", instrument = INSTRUMENT_CP)
    public DataPage getAccountById(RequestEntity<String> requestEntity, @PathVariable String accountId, //
            @PathVariable Predefined attributeGroup) {
        return getAccountById(requestEntity, accountId, attributeGroup, null);
    }

    @GetMapping(value = "/{accountId}/{attributeGroup}/danteformat")
    @ResponseBody
    @ApiOperation(value = "Get account with attributes of the attribute group by its Id in dante format")
    @InvocationMeter(name = "talkingpoint-dante", measurment = "ulysses", instrument = INSTRUMENT_TP)
    @InvocationMeter(name = "companyprofile-dante", measurment = "ulysses", instrument = INSTRUMENT_CP)
    public FrontEndResponse<String> getAccountByIdInDanteFormat(RequestEntity<String> requestEntity,
            @PathVariable String accountId, //
            @PathVariable Predefined attributeGroup) {
        try {
            CustomerSpace customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId());
            List<String> requiredAttributes = Collections.singletonList(InterfaceName.SpendAnalyticsSegment.name());
            DataPage accountRawData = getAccountById(requestEntity, accountId, attributeGroup, requiredAttributes);
            if (accountRawData.getData().size() != 1) {
                String message = MessageFormat.format(LedpCode.LEDP_39003.getMessage(), "Account", 0);
                log.warn("Failed to get account data for account id: " + message);
                return new FrontEndResponse<>(new ErrorDetails(LedpCode.LEDP_39003, message, null));
            }
            AccountDanteFormatter accountFormatter = accountDanteFormatterProvider.get();
            accountFormatter.setIsEntityMatchEnabled(batonService.isEntityMatchEnabled(customerSpace));
            return new FrontEndResponse<>(accountFormatter.format(accountRawData.getData().get(0)));
        } catch (LedpException le) {
            log.error("Failed to get account data for account id: " + accountId, le.getMessage());
            return new FrontEndResponse<>(le.getErrorDetails());
        } catch (Exception e) {
            log.error("Failed to get account data for account id: " + accountId, e);
            return new FrontEndResponse<>(new LedpException(LedpCode.LEDP_00002, e).getErrorDetails());
        }
    }

    @GetMapping(value = "/{accountId}/{attributeGroup}/danteformat/aslist")
    @ResponseBody
    @ApiOperation(value = "Get account with attributes of the attribute group by its Id in dante format")
    @InvocationMeter(name = "talkingpoint-dante-list", measurment = "ulysses", instrument = INSTRUMENT_TP)
    @InvocationMeter(name = "companyprofile-dante-list", measurment = "ulysses", instrument = INSTRUMENT_CP)
    public FrontEndResponse<List<String>> getAccountsByIdInDanteFormat(RequestEntity<String> requestEntity,
            @PathVariable String accountId, //
            @PathVariable Predefined attributeGroup) {
        try {
            DataPage accountRawData = getAccountById(requestEntity, accountId, attributeGroup);
            if (accountRawData.getData().size() != 1) {
                String message = MessageFormat.format(LedpCode.LEDP_39003.getMessage(), "Account", 0);
                log.warn("Failed to get account data for account id: " + message);
                return new FrontEndResponse<>(new ErrorDetails(LedpCode.LEDP_39003, message, null));
            }
            CustomerSpace customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId());
            AccountDanteFormatter accountFormatter = accountDanteFormatterProvider.get();
            accountFormatter.setIsEntityMatchEnabled(batonService.isEntityMatchEnabled(customerSpace));
            return new FrontEndResponse<>(
                    Collections.singletonList(accountFormatter.format(accountRawData.getData().get(0))));
        } catch (LedpException le) {
            log.error("Failed to get account data for account id: " + accountId, le);
            return new FrontEndResponse<>(le.getErrorDetails());
        } catch (Exception e) {
            log.error("Failed to get account data for account id: " + accountId, e);
            return new FrontEndResponse<>(new LedpException(LedpCode.LEDP_00002, e).getErrorDetails());
        }
    }

    @GetMapping(value = "/spendanalyticssegments/danteformat")
    @ResponseBody
    @ApiOperation(value = "Get account with attributes of the attribute group by its Id in dante format")
    public FrontEndResponse<List<String>> getAccountSegmentsInDanteFormat() {
        String customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId()).toString();
        try {
            AccountDanteFormatter accountFormatter = accountDanteFormatterProvider.get();
            accountFormatter
                    .setIsEntityMatchEnabled(batonService.isEntityMatchEnabled(CustomerSpace.parse(customerSpace)));
            return new FrontEndResponse<>(accountFormatter
                    .format(periodTransactionProxy.getAllSpendAnalyticsSegments(customerSpace).getData()));
        } catch (LedpException le) {
            log.error("Failed to get spend analytics segments for customerspace : " + customerSpace, le);
            return new FrontEndResponse<>(le.getErrorDetails());
        } catch (Exception e) {
            log.error("Failed to get spend analytics segments for customerSpace: " + customerSpace, e);
            return new FrontEndResponse<>(new LedpException(LedpCode.LEDP_00002, e).getErrorDetails());
        }
    }

    @GetMapping(value = "/{accountId}/plays/{playId}/talkingpoints/danteformat")
    @ResponseBody
    @ApiOperation(value = "Get account with attributes of the attribute group by its Id ")
    @InvocationMeter(name = "customlist-dante", measurment = "ulysses", instrument = INSTRUMENT_CL)
    public FrontEndResponse<List<String>> getAccountsAndTalkingpoints(RequestEntity<String> requestEntity,
            @PathVariable String accountId, //
            @PathVariable String playId) {
        String customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId()).getTenantId();
        try {
            List<TalkingPointDTO> tps = talkingPointProxy.findAllByPlayName(customerSpace, playId, true);
            AccountDanteFormatter accountFormatter = accountDanteFormatterProvider.get();
            accountFormatter
                    .setIsEntityMatchEnabled(batonService.isEntityMatchEnabled(CustomerSpace.parse(customerSpace)));
            Set<String> requiredAccountAttributes = accountFormatter.getUIRequiredProperties();
            if (CollectionUtils.isEmpty(tps))
                log.warn(MessageFormat.format(LedpCode.LEDP_39009.getMessage(), playId, customerSpace));
            else {
                Set<String> extractedAccountAttributes = ExtractAttributes(tps, BusinessEntity.Account);
                requiredAccountAttributes.addAll(extractedAccountAttributes);
            }

            DataPage accountRawData = dataLakeService.getAccountById(accountId,
                    new ArrayList<>(requiredAccountAttributes), tenantProxy.getOrgInfoFromOAuthRequest(requestEntity));
            if (accountRawData.getData().size() != 1) {
                String message = MessageFormat.format(LedpCode.LEDP_39003.getMessage(), "Account", 0);
                log.warn("Failed to get account data for account id: " + message);
                return new FrontEndResponse<>(new ErrorDetails(LedpCode.LEDP_39003, message, null));
            }

            List<String> toReturn = talkingPointDanteFormatter
                    .format(JsonUtils.convertList(tps, TalkingPointDTO.class));
            toReturn.add(accountFormatter.format(accountRawData.getData().get(0)));
            return new FrontEndResponse<>(toReturn);
        } catch (LedpException le) {
            log.error("Failed to populate talkingpoints and accounts for " + customerSpace, le);
            return new FrontEndResponse<>(le.getErrorDetails());
        } catch (Exception e) {
            log.error("Failed to get spend analytics segments for customerSpace: " + customerSpace, e);
            return new FrontEndResponse<>(new LedpException(LedpCode.LEDP_00002, e).getErrorDetails());
        }
    }

    private Set<String> ExtractAttributes(List<TalkingPointDTO> tps, BusinessEntity entity) {
        return tps.stream().map(TalkingPointDTO::getAttributes).flatMap(Collection::stream)
                .filter(attr -> attr.getEntity() == entity).map(AttributeLookup::getAttribute)
                .collect(Collectors.toSet());
    }

    private DataPage getAccountById(RequestEntity<String> requestEntity, String accountId, Predefined attributeGroup,
            List<String> requiredAttributes) {
        return CollectionUtils.isEmpty(requiredAttributes) //
                ? dataLakeService.getAccountById(accountId, attributeGroup,
                        tenantProxy.getOrgInfoFromOAuthRequest(requestEntity))
                : dataLakeService.getAccountById(accountId, attributeGroup,
                        tenantProxy.getOrgInfoFromOAuthRequest(requestEntity), requiredAttributes);
    }

    private class AccountAndTalkingPoints {
        private String accountStr;
        private List<String> talkingpointstrs;

        AccountAndTalkingPoints(String accountStr, List<String> talkingpointstrs) {
            this.accountStr = accountStr;
            this.talkingpointstrs = talkingpointstrs;
        }

        @JsonProperty("account")
        public String getAccountStr() {
            return accountStr;
        }

        @JsonProperty("talkingPoints")
        public List<String> getTalkingpointstrs() {
            return talkingpointstrs;
        }

    }
}
