package com.latticeengines.ldc_collectiondb;

import com.latticeengines.ldc_collectiondb.dao.CollectionRequestService;
import com.latticeengines.ldc_collectiondb.dao.RawCollectionRequestService;
import com.latticeengines.ldc_collectiondb.entity.CollectionRequest;
import com.latticeengines.ldc_collectiondb.entity.RawCollectionRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceContext;
import java.sql.Timestamp;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@DirtiesContext
@ContextConfiguration(locations = {"classpath:test-context.xml"})
public class TestRawCollectionRequest extends AbstractTestNGSpringContextTests {
    @Autowired
    RawCollectionRequestService rawCollectionRequestService;
    @Autowired
    CollectionRequestService collectionRequestService;
    //@PersistenceContext(name="ldc_collectionEntityManagerFactory")
    //LocalContainerEntityManagerFactoryBean emf;


    @Test(groups = "functional")
    public void testReqTransfer() throws Exception
    {
        initRawReqs(6, 50, 100);

        //get non transferred req
        List<RawCollectionRequest> nonTransferred = rawCollectionRequestService.getNonTransferred();
        System.out.println("non transferred reqs: " + nonTransferred.size());

        List<CollectionRequest> curReqs = collectionRequestService.getAll();
        System.out.println("current colleciton reqs: " + curReqs.size());

        BitSet filter = collectionRequestService.add(nonTransferred);
        System.out.println("transfer " + nonTransferred.size() + " reqs, " + filter.cardinality() + " reqs filtered");

        curReqs = collectionRequestService.getAll();
        System.out.println("current collection reqs: " + curReqs.size());

        rawCollectionRequestService.updateTransferredStatus(nonTransferred, filter, true);
        nonTransferred = rawCollectionRequestService.getNonTransferred();
        System.out.println("non transferred reqs: " + nonTransferred.size());
    }

    @Test(groups = "unit")
    public void testRawReq() {
        /*
        //ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("test-context.xml");
        //LocalContainerEntityManagerFactoryBean emf = (LocalContainerEntityManagerFactoryBean) ctx.getBean(LocalContainerEntityManagerFactoryBean.class);
        EntityManager em = emf.getNativeEntityManagerFactory().createEntityManager();
        em.getTransaction().begin();*/

        RawCollectionRequestService service = rawCollectionRequestService;
        //get non transferred req
        List<RawCollectionRequest> nonTransferred = rawCollectionRequestService.getNonTransferred();
        System.out.println("non transferred reqs: " + nonTransferred.size());

        //create
        RawCollectionRequest rcr = new RawCollectionRequest();
        rcr.setDomain("google.com");
        rcr.setOriginalRequestId("alpha_163");
        rcr.setRequestedTime(new Timestamp(System.currentTimeMillis()));
        rcr.setTransferred(false);
        rcr.setVendor("builtwith");
        System.out.println("new req id before insertion: " + rcr.getPid());

        service.save(rcr);
        //em.persist(rcr);
        System.out.println("new req id after insertion: " + rcr.getPid());

        RawCollectionRequest rcr1 = service.getById(rcr.getPid());
        System.out.println("retrieved newly stored req's domain = " + rcr1.getDomain());

        StackTraceElement ste = Thread.currentThread().getStackTrace()[1];
        System.out.println(ste.getClassName() + "." + ste.getMethodName() + " complete");
    }

    static final String[] vendors = new String[] {"builtwith", "alexa", "compete", "feature", "hpa_new", "semrush", "ORBINTELLIGENCEV2"};
    static final int domain_ul = 100;
    void initRawReqs(int vendor_count, int domain_count, int req_count) throws Exception
    {
        System.out.println("passed vendor count: " + vendor_count + ", domain count: " + domain_count);
        if (vendor_count > vendors.length)
            vendor_count = vendors.length;
        if (domain_count > domain_ul)
            domain_count = domain_ul;
        int[] domain_pool = new int[domain_count];
        for (int i = 0; i < domain_count; ++i)
            domain_pool[i] = ThreadLocalRandom.current().nextInt(0, domain_ul);
        String[] vendor_pool = new String[vendor_count];
        for (int i = 0; i < vendor_count; ++i)
            vendor_pool[i] = vendors[ThreadLocalRandom.current().nextInt(0, vendors.length)];
        System.out.println("using vendor count: " + vendor_count + ", domain count: " + domain_count);

        for (int i = 0; i < req_count; ++i) {
            int vendor_idx = ThreadLocalRandom.current().nextInt(0, vendor_count);
            int domain_idx = ThreadLocalRandom.current().nextInt(0, domain_count);

            RawCollectionRequest rcr = new RawCollectionRequest();
            rcr.setDomain("domain." + domain_pool[domain_idx]);
            rcr.setOriginalRequestId("alpha_163");
            rcr.setRequestedTime(new Timestamp(System.currentTimeMillis()));
            rcr.setTransferred(false);
            rcr.setVendor(vendor_pool[vendor_idx]);

            rawCollectionRequestService.save(rcr);
            //Thread.sleep(1000);
        }
        System.out.println("generate " + req_count + " raw reqs");
    }

}
