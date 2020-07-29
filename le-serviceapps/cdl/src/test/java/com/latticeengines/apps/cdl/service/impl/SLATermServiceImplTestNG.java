package com.latticeengines.apps.cdl.service.impl;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicReference;

import javax.inject.Inject;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.SLAFulfillmentService;
import com.latticeengines.apps.cdl.service.SLATermService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.apps.core.service.ActionService;
import com.latticeengines.common.exposed.util.CronUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.cdl.sla.ActionConfigPredicate;
import com.latticeengines.domain.exposed.cdl.sla.ActionPredicate;
import com.latticeengines.domain.exposed.cdl.sla.SLAFulfillment;
import com.latticeengines.domain.exposed.cdl.sla.SLATerm;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.CleanupActionConfiguration;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class SLATermServiceImplTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(SLATermServiceImplTestNG.class);

    private static final String ACTION_INITIATOR = "test@lattice-engines.com";

    @Inject
    private SLATermService slaTermService;
    @Inject
    private SLAFulfillmentService slaFulfillmentService;
    @Inject
    private ActionService actionService;

    private String termName = "termName1";
    private String newTermName = "newTermName";
    private int version = 1;
    private String cron = "0 0 12 * * ?";
    private SLATerm term;
    private SLAFulfillment fulfillment;
    private RetryTemplate retry;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
        retry = RetryUtils.getRetryTemplate(10, //
                Collections.singleton(AssertionError.class), null);
    }

    @Test(groups = "functional")
    public void testCreateSLATerm() {
        ActionConfigPredicate configPredicate = new ActionConfigPredicate();
        configPredicate.setJsonProperty("impact_entities");
        configPredicate.setExpectedValues(Arrays.asList("Account", "Contact"));
        ActionPredicate predicate = new ActionPredicate();
        predicate.setActionType(ActionType.CDL_OPERATION_WORKFLOW);
        predicate.setActionConfigPredicates(Collections.singletonList(configPredicate));
        term = new SLATerm();
//        term.setTenant(mainTestTenant);
        term.setTermName(termName);
        term.setVersion(version);
        String duration = "43200000";
        term.setDeliveryDuration(duration);
        term.setScheduleCron(cron);
        term.setPredicates(Collections.singletonList(predicate));
        String timeZone = "Asia/Shanghai";
        term.setTimeZone(timeZone);
        term.setSlaTermType(SLATerm.SLATermType.Daily);
        term = slaTermService.createOrUpdate(mainCustomerSpace, term);
        log.info("SLATerm is {}.", JsonUtils.serialize(term));
        log.info("pid is {}", term.getPid());
        Assert.assertNotNull(term.getPid());
    }

    @Test(groups = "functional", dependsOnMethods = "testCreateSLATerm")
    public void testCreateSLAFulfillment() {
        fulfillment = new SLAFulfillment();
        fulfillment.setTenant(mainTestTenant);
        fulfillment.setAction(generateAction());
        fulfillment.setSlaVersion(version);
        fulfillment.setTerm(term);
        if (CronUtils.isValidExpression(cron)) {
            DateTime time = CronUtils.getNextFireTime(cron);
            log.info("term is {}.", JsonUtils.serialize(term));
            log.info("timeZone is {}.", TimeZone.getTimeZone(term.getTimeZone()));
            Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(term.getTimeZone()));
            calendar.setTime(time.toDate());
            calendar.getTimeInMillis();
            fulfillment.setEarliestKickoffTime(calendar.getTimeInMillis());
            long deadline = calendar.getTimeInMillis() + Long.valueOf(term.getDeliveryDuration());
            fulfillment.setDeliveryDeadline(deadline);
            fulfillment.setFulfillmentStatus(SLAFulfillment.SLAFulfillmentStatus.Pending);
        }
        fulfillment = slaFulfillmentService.createOrUpdate(mainCustomerSpace, fulfillment);
        log.info("SLAFulfillment is {}.", JsonUtils.serialize(fulfillment));
        log.info("pid is {}", fulfillment.getPid());
        Assert.assertNotNull(fulfillment.getPid());
    }

    @Test(groups = "functional", dependsOnMethods = "testCreateSLAFulfillment")
    public void testUpdateSlaTerm() {
        AtomicReference<SLATerm> createdAtom = new AtomicReference<>();
        retry.execute(context -> {
            createdAtom.set(slaTermService.findByPid(mainCustomerSpace, term.getPid()));
            Assert.assertNotNull(createdAtom.get());
            return true;
        });
        SLATerm slaTerm = createdAtom.get();
        Assert.assertNotNull(slaTerm);
        Assert.assertEquals(slaTerm.getTermName(), termName);
        slaTerm.setTermName(newTermName);
        slaTermService.createOrUpdate(mainCustomerSpace, slaTerm);
        retry.execute(context -> {
            createdAtom.set(slaTermService.findByPid(mainCustomerSpace, term.getPid()));
            Assert.assertEquals(createdAtom.get().getTermName(), newTermName);
            return true;
        });
        slaTerm = createdAtom.get();
        Assert.assertEquals(slaTerm.getTermName(), newTermName);
    }

    @Test(groups = "functional", dependsOnMethods = "testUpdateSlaTerm")
    public void testUpdateSlaFulfillment() {
        AtomicReference<SLAFulfillment> createdAtom = new AtomicReference<>();
        retry.execute(context -> {
            createdAtom.set(slaFulfillmentService.findByPid(mainCustomerSpace, fulfillment.getPid()));
            Assert.assertNotNull(createdAtom.get());
            return true;
        });
        SLAFulfillment slaFulfillment = createdAtom.get();
        Assert.assertNotNull(slaFulfillment);
        Assert.assertEquals(SLAFulfillment.SLAFulfillmentStatus.Pending, slaFulfillment.getFulfillmentStatus());
        slaFulfillment.setFulfillmentStatus(SLAFulfillment.SLAFulfillmentStatus.Violated);
        slaFulfillmentService.createOrUpdate(mainCustomerSpace, slaFulfillment);
        AtomicReference<List<SLAFulfillment>> createdAtom1 = new AtomicReference<>();
        retry.execute(context -> {
            createdAtom1.set(slaFulfillmentService.findByTenant(mainCustomerSpace));
            Assert.assertEquals(createdAtom1.get().get(0).getFulfillmentStatus(), SLAFulfillment.SLAFulfillmentStatus.Violated);
            return true;
        });
        slaFulfillment = createdAtom1.get().get(0);
        Assert.assertEquals(slaFulfillment.getFulfillmentStatus(), SLAFulfillment.SLAFulfillmentStatus.Violated);
    }

    private Action generateAction() {
        Action action = new Action();
        action.setTenant(mainTestTenant);
        action.setActionInitiator(ACTION_INITIATOR);
        action.setType(ActionType.CDL_OPERATION_WORKFLOW);
        CleanupActionConfiguration actionConfig = new CleanupActionConfiguration();
        actionConfig.setImpactEntities(Collections.singletonList(BusinessEntity.Account));
        action.setActionConfiguration(actionConfig);
        actionService.create(action);
        return action;
    }
}
