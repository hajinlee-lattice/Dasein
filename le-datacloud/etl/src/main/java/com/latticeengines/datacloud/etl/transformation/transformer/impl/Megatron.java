package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.core.util.RequestContext;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.MegatronConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

@Component("megatron")
class Megatron extends AbstractTransformer<MegatronConfig> {

    private static final Logger log = LoggerFactory.getLogger(Megatron.class);

    @Inject
    protected Configuration yarnConfiguration;

    private static final String avroName = "/seed.avro";

    private static int ORPHAN = 1;
    private static int WRONGDU = 2;
    private static int DUPPD = 4;
    private static int MISSINGPD = 8;
    private static int OOB = 16;

    private Random random = new Random();

    @Autowired
    protected HdfsPathBuilder hdfsPathBuilder;

    @Override
    public boolean validateConfig(MegatronConfig config, List<String> baseSources) {
        String error = null;
        int numAccounts = config.getNumAccounts();
        if ((numAccounts < 4 * 1024) || (numAccounts > 80 * 1024)) {
            error = String.format("Account number is not range [%d, %d]", 4 * 1024, 80 * 1024);
            log.error(error);
            RequestContext.logError(error);
            return false;
        }
        return true;
    }

    @Override
    protected Class<? extends TransformerConfig> getConfigurationClass() {
        return MegatronConfig.class;
    }

    @Override
    public String getName() {
        return "megatron";
    }

    @SuppressWarnings("finally")
    @Override
    protected boolean transformInternal(TransformationProgress progress, String workflowDir,
            TransformStep step) {
        String confStr = step.getConfig();
        MegatronConfig configuration = getConfiguration(confStr);

        Schema seedSchema;

        try {
            InputStream is = Thread.currentThread().getContextClassLoader()
                    .getResourceAsStream("schema/MockAmSeed.avsc");
            Schema.Parser parser = new Schema.Parser();
            seedSchema = parser.parse(is);
        } catch (IOException e) {
            log.error("Message key schema avsc file not found", e);
            seedSchema = null;
        }

        MegatronContext context = new MegatronContext(configuration, seedSchema);

        try {
            createAccounts(context);
        } catch (Exception e) {
            log.error("Failed to create accounts", e);
        } finally {
            if (context.getNumAccounts() == configuration.getNumAccounts()) {
                try {

                    List<GenericData.Record> accounts = context.getAllAccounts();
                    List<GenericRecord> finalAccounts = new ArrayList<GenericRecord>();
                    for (GenericData.Record account : accounts) {
                        String domain = (String) account.get("Domain");
                        if (domain != null) {
                            account.put("Domain", domain + ".com");
                        }
                        finalAccounts.add(account);
                    }
                    log.info("flushing " + finalAccounts.size() + " acounts");
                    AvroUtils.writeToHdfsFile(yarnConfiguration, context.getSchema(),
                            workflowDir + avroName, finalAccounts);
                } catch (Exception e) {
                    log.error("Failed to write to avro file", e);
                }
                return true;
            } else {
                log.error("Not all accounts are created" + context.getNumAccounts() + " "
                        + configuration.getNumAccounts());
                return false;
            }
        }
    }

    private void createAccounts(MegatronContext context) throws Exception {
        MegatronConfig config = context.getConfig();
        while (context.getNumAccounts() < config.getNumAccounts()) {
            createOneSet(context);
        }

        messupSomePDs(context);
        oobSomeAccounts(context);

    }

    private void createOneSet(MegatronContext context) throws Exception {
        GenericData.Record dmHQ = createDmHQ(context);

        if (domainOnly(context, dmHQ)) {
            return;
        }

        if (!dunsOnly(context, dmHQ)) {
            addSomeDomains(context, dmHQ, 0);
        }

        addSomeSubs(context, dmHQ);

        messupFirmo(context, dmHQ);

        addSomeOrphans(context, dmHQ);

    }

    private void oobSomeAccounts(MegatronContext context) {
        if (!context.checkFlag(OOB)) {
            return;
        }
        List<GenericData.Record> accounts = context.getAllAccounts();
        for (GenericData.Record account : accounts) {
            if (random.nextInt(16) == 1) {
                tagAccount(account, OOB);
            }
        }
    }

    private void messupSomePDs(MegatronContext context) {
        List<GenericData.Record> accounts = context.getAllAccounts();
        for (GenericData.Record account : accounts) {
            List<GenericData.Record> domains = context.getDomains(account);
            if (domains == null) {
                if (!context.checkFlag(MISSINGPD)) {
                    continue;
                }
                if (random.nextInt(16) == 8) {
                    account.put("Domain", null);
                    tagAccount(account, MISSINGPD);
                }
                continue;
            }
            if (!context.checkFlag(DUPPD)) {
                continue;
            }
            if (random.nextInt(16) != 8) {
                continue;
            }
            for (GenericData.Record domain : domains) {
                domain.put("LE_IS_PRIMARY_DOMAIN", "Y");
                tagAccount(domain, DUPPD);
            }
        }
    }

    private void addSomeOrphans(MegatronContext context, GenericData.Record dmHQ) throws Exception {
        if (!context.checkFlag(ORPHAN)) {
            return;
        }
        int orphans = random.nextInt(6);
        orphans = (orphans < 3) ? 0 : orphans - 3;
        for (int i = 0; i < orphans; i++) {
            GenericData.Record orphan = context.cloneAccountWithNewDUNS(dmHQ);
            orphan.put("LE_NUMBER_OF_LOCATIONS", 0);
            tagAccount(orphan, ORPHAN);
            context.addAccount(null, orphan);
        }
    }

    private void messupFirmo(MegatronContext context, GenericData.Record dmHQ) {
        if (!context.checkFlag(WRONGDU)) {
            return;
        }
        List<GenericData.Record> accounts = context.getChildren(dmHQ);

        if (accounts == null) {
            return;
        }

        if (random.nextInt(16) != 1) {
            return;
        }

        String employeeStr = (String) dmHQ.get("LE_EMPLOYEE_RANGE");
        Integer location = (Integer) dmHQ.get("LE_NUMBER_OF_LOCATIONS");
        String revenueStr = (String) dmHQ.get("LE_REVENUE_RANGE");
        String industryStr = (String) dmHQ.get("LE_INDUSTRY");

        for (GenericData.Record account : accounts) {
            int employees = random.nextInt(12);
            account.put("LE_EMPLOYEE_RANGE", employees + "");
            int locations = (employees == 1) ? 1 : random.nextInt(16);
            account.put("LE_NUMBER_OF_LOCATIONS", ((locations < 4) ? 1 : locations - 3));
            account.put("LE_REVENUE_RANGE", random.nextInt(16) + "");
            account.put("LE_INDUSTRY", random.nextInt(16) + "");
            if (!(employeeStr.equals((String) account.get("LE_EMPLOYEE_RANGE")))
                    || !(location == ((Integer) account.get("LE_NUMBER_OF_LOCATIONS")))
                    || !(revenueStr.equals((String) account.get("LE_REVENUE_RANGE")))
                    || !(industryStr.equals((String) account.get("LE_INDUSTRY")))) {
                tagAccount(account, WRONGDU);
            }
        }
    }

    private GenericData.Record createDmHQ(MegatronContext context) throws Exception {
        GenericData.Record dmHQ = context.createAccount();
        context.addAccount(null, dmHQ);
        return dmHQ;
    }

    private boolean domainOnly(MegatronContext context, GenericData.Record account) {
        if (random.nextInt(5) == 1) {
            account.put("DUNS", null);
            return true;
        } else {
            return false;
        }
    }

    private boolean dunsOnly(MegatronContext context, GenericData.Record account) {
        if (random.nextInt(5) == 1) {
            account.put("Domain", null);
            return true;
        } else {
            return false;
        }
    }

    private void addSomeDomains(MegatronContext context, GenericData.Record account, int sub)
            throws Exception {
        int numDomains = random.nextInt(12);

        numDomains = (numDomains <= 4 ? 0 : numDomains - 4);

        String domain = (String) account.get("Domain");

        for (int i = 0; i < numDomains; i++) {
            GenericData.Record child = context.cloneAccount(account);
            child.put("Domain", domain + "_" + sub + "_" + i);
            child.put("LE_IS_PRIMARY_DOMAIN", "N");
            context.addDomain(account, child);
        }
    }

    public void addSomeSubs(MegatronContext context, GenericData.Record dmHQ) throws Exception {

        if ((((String) dmHQ.get("LE_EMPLOYEE_RANGE")).equals("0"))
                || (((String) dmHQ.get("LE_EMPLOYEE_RANGE")).equals("1"))) {
            return;
        }

        int numSubs = random.nextInt(16);

        List<GenericData.Record> domains = context.getDomains(dmHQ);

        for (int i = 0; i < numSubs; i++) {
            GenericData.Record sub = context.cloneAccountWithNewDUNS(dmHQ);
            sub.put("LE_IS_PRIMARY_LOCATION", "N");
            context.addAccount(dmHQ, sub);

            if (dunsOnly(context, sub)) {
                continue;
            }

            if (domains != null) {
                for (int j = 0; j < domains.size(); j++) {
                    GenericData.Record domain = context.cloneAccount(sub);
                    domain.put("Domain", (domains.get(j)).get("Domain"));
                    domain.put("LE_IS_PRIMARY_DOMAIN", "N");
                    context.addDomain(sub, domain);
                }
            }

            if (random.nextInt(3) == 1) {
                addSomeDomains(context, sub, i + 1);
            }
        }
    }

    private void tagAccount(GenericData.Record account, int flag) {
        int origFlag = (Integer) account.get("MockFlag");
        account.put("MockFlag", flag | origFlag);
    }

    class MegatronContext {

        private MegatronConfig config;
        private Schema schema;

        private long currentAccountId;

        private int currentDuns;

        private int currentDomain;

        private int currentName;

        private int currentGDuns;

        private int flags;
        private Random random = new Random();

        Map<Long, List<GenericData.Record>> domainMap;

        Map<Long, List<GenericData.Record>> hqMap;

        List<GenericData.Record> accounts;

        public MegatronContext(MegatronConfig config, Schema schema) {
            this.config = config;
            this.schema = schema;

            Integer flags = config.getFlags();

            if (flags == null) {
                this.flags = ORPHAN | WRONGDU | MISSINGPD | DUPPD;
            } else {
                this.flags = flags;
            }

            hqMap = new HashMap<Long, List<GenericData.Record>>();
            domainMap = new HashMap<Long, List<GenericData.Record>>();
            accounts = new ArrayList<GenericData.Record>();

            currentAccountId = 0;
            currentDuns = 0;
            currentDomain = 0;
            currentGDuns = 0;
            currentName = 0;
        }

        public boolean checkFlag(int flag) {
            return ((flags & flag) != 0);
        }

        public Schema getSchema() {
            return schema;
        }

        public MegatronConfig getConfig() {
            return config;
        }

        public int getNumAccounts() {
            return accounts.size();
        }

        @SuppressWarnings("unlikely-arg-type")
        public void addAccount(GenericData.Record parent, GenericData.Record child) {

            if (parent != null) {
                Long pid = (Long) parent.get("LatticeID");
                List<GenericData.Record> childList = hqMap.get(parent);
                if (childList == null) {
                    childList = new ArrayList<GenericData.Record>();
                    hqMap.put(pid, childList);
                }
                childList.add(child);
            }

            accounts.add(child);
        }

        public void addDomain(GenericData.Record parent, GenericData.Record child) {

            Long pid = (Long) parent.get("LatticeID");
            List<GenericData.Record> childList = domainMap.get(pid);
            if (childList == null) {
                childList = new ArrayList<GenericData.Record>();
                domainMap.put(pid, childList);
            }
            childList.add(child);

            accounts.add(child);
        }

        public List<GenericData.Record> getAllAccounts() {
            return accounts;

        }

        public Map<Long, List<GenericData.Record>> getAllDomain() {
            return domainMap;
        }

        public List<GenericData.Record> getChildren(GenericData.Record dmHQ) {
            Long pid = (Long) dmHQ.get("LatticeID");
            List<GenericData.Record> childList = domainMap.get(pid);
            return childList;
        }

        public List<GenericData.Record> getDomains(GenericData.Record account) {
            Long pid = (Long) account.get("LatticeID");
            return domainMap.get(pid);
        }

        public GenericData.Record createAccount() throws Exception {
            if (getNumAccounts() >= config.getNumAccounts()) {
                throw new Exception("Enough accounts");
            }

            GenericData.Record account = new GenericData.Record(schema);

            boolean sameGlobalDuns = (currentDuns == 0) ? false
                    : ((random.nextInt(2) == 0) ? true : false);

            if (!sameGlobalDuns) {
                currentGDuns = currentDuns;
            }

            account.put("LatticeID", currentAccountId);

            account.put("LE_PRIMARY_DUNS", currentDuns + "");
            account.put("DUNS", currentDuns + "");
            account.put("GLOBAL_ULTIMATE_DUNS_NUMBER", currentGDuns + "");

            account.put("Domain", "Domain" + currentDomain);
            account.put("LE_IS_PRIMARY_DOMAIN", "Y");

            account.put("LE_IS_PRIMARY_LOCATION", "Y");
            account.put("Name", currentName + "");

            account.put("LE_COUNTRY", "USA");
            account.put("ZipCode", random.nextInt(4096) + "");
            account.put("Street", random.nextInt(4096) + "");
            account.put("City", random.nextInt(4096) + "");
            String state;
            switch (random.nextInt(6)) {
                case 0:
                    state = "NY";
                    break;
                case 1:
                    state = "NH";
                    break;
                default:
                    state = "CA";
            }
            account.put("State", state);
            account.put("Country", "USA");
            account.put("LE_COMPANY_PHONE", random.nextInt(4096) + "");

            int employees = random.nextInt(8);
            employees = (employees < 4) ? 1 : (employees - 3);
            account.put("LE_EMPLOYEE_RANGE", employees + "");
            int locations = (employees == 1) ? 1 : random.nextInt(16);
            account.put("LE_NUMBER_OF_LOCATIONS", ((locations < 4) ? 1 : (locations - 3)));
            account.put("LE_REVENUE_RANGE", random.nextInt(16) + "");

            account.put("LE_INDUSTRY", random.nextInt(16) + "");

            account.put("LE_COMPANY_DESCRIPTION", "What So ever");
            account.put("LE_SIC_CODE", random.nextInt(4096) + "");
            account.put("LE_NAICS_CODE", +random.nextInt(4096) + "");
            account.put("LE_Last_Upload_Date", System.currentTimeMillis());

            account.put("MockFlag", 0);

            currentAccountId++;
            currentDomain++;
            currentDuns++;
            currentName++;

            return account;
        }

        public GenericData.Record cloneAccount(GenericData.Record other) throws Exception {
            if (getNumAccounts() >= config.getNumAccounts()) {
                throw new Exception("Enough accounts");
            }

            GenericData.Record account = new GenericData.Record(other, false);
            account.put("LatticeID", currentAccountId++);
            return account;
        }

        public GenericData.Record cloneAccountWithNewDUNS(GenericData.Record other)
                throws Exception {
            if (getNumAccounts() >= config.getNumAccounts()) {
                throw new Exception("Enough accounts");
            }

            GenericData.Record account = new GenericData.Record(other, false);
            account.put("LatticeID", currentAccountId++);
            account.put("DUNS", currentDuns + "");
            currentDuns++;
            return account;
        }
    }
}
