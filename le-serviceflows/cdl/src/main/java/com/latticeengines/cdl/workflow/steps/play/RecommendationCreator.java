package com.latticeengines.cdl.workflow.steps.play;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;
import com.latticeengines.domain.exposed.playmaker.PlaymakerUtils;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.RuleBucketName;
import com.latticeengines.domain.exposed.security.Tenant;

@Component
public class RecommendationCreator {

    private static final Logger log = LoggerFactory.getLogger(PlayLaunchProcessor.class);

    private static final double DEFAULT_LIKELIHOOD = 50.0D;

    public void generateRecommendations(PlayLaunchContext playLaunchContext, List<Map<String, Object>> accountList,
            Map<Object, List<Map<String, String>>> mapForAccountAndContactList,
            DataFileWriter<GenericRecord> dataFileWriter) {

        List<Recommendation> recommendations = accountList//
                .stream().parallel() //
                .map( //
                        account -> {
                            try {
                                return processSingleAccount(playLaunchContext, //
                                        mapForAccountAndContactList, account);
                            } catch (Throwable th) {
                                log.error(th.getMessage(), th);
                                playLaunchContext.getCounter().getAccountErrored().addAndGet(1);
                                return null;
                            }
                        }) //
                .filter(rec -> rec != null) //
                .collect(Collectors.toList());

        if (CollectionUtils.isNotEmpty(recommendations)) {
            List<GenericRecord> records = //
                    recommendations.stream() //
                            .map(rec -> createRecommendationRecord(dataFileWriter, playLaunchContext.getSchema(), rec)) //
                            .collect(Collectors.toList());

            records.stream() //
                    .forEach(datum -> {
                        try {
                            dataFileWriter.append(datum);
                        } catch (IOException e) {
                            log.error(String.format("Error while writing recommendation record (%s) to avro file",
                                    JsonUtils.serialize(datum)), e);
                        }
                    });
        }
    }

    private GenericRecord createRecommendationRecord(DataFileWriter<GenericRecord> dataFileWriter, Schema schema,
            Recommendation recommendation) {
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        Map<String, Object> recMap = Recommendation.convertToMap(recommendation);

        for (Field field : schema.getFields()) {
            String fieldName = field.name();
            builder.set(fieldName, recMap.get(fieldName));
        }
        return builder.build();
    }

    private Recommendation processSingleAccount(PlayLaunchContext playLaunchContext,
            Map<Object, List<Map<String, String>>> mapForAccountAndContactList, Map<String, Object> account) {
        RuleBucketName bucket = getBucketInfo(playLaunchContext, account);
        Recommendation recommendation = null;

        // Generate recommendation only when bucket is selected for launch
        if (playLaunchContext.getPlayLaunch().getBucketsToLaunch().contains(bucket)) {

            // prepare recommendation
            recommendation = //
                    prepareRecommendation(playLaunchContext, account, mapForAccountAndContactList, bucket);

            // update corresponding counters
            playLaunchContext.getCounter().getContactLaunched().addAndGet(
                    recommendation.getExpandedContacts() != null ? recommendation.getExpandedContacts().size() : 0);
            playLaunchContext.getCounter().getAccountLaunched().addAndGet(1);
        } else {
            playLaunchContext.getCounter().getAccountSuppressed().addAndGet(1);
        }

        return recommendation;
    }

    private RuleBucketName getBucketInfo(PlayLaunchContext playLaunchContext, Map<String, Object> account) {
        String bucketName = checkAndGet(account, playLaunchContext.getModelId());
        RuleBucketName bucket = RuleBucketName.getRuleBucketName(bucketName);

        if (bucket == null) {
            bucket = RuleBucketName.valueOf(bucketName);
        }
        return bucket;
    }

    private Recommendation prepareRecommendation(PlayLaunchContext playLaunchContext, Map<String, Object> account,
            Map<Object, List<Map<String, String>>> mapForAccountAndContactList, RuleBucketName bucket) {
        PlayLaunch playLaunch = playLaunchContext.getPlayLaunch();
        long launchTimestampMillis = playLaunchContext.getLaunchTimestampMillis();
        String playName = playLaunchContext.getPlayName();
        String playLaunchId = playLaunchContext.getPlayLaunchId();
        Tenant tenant = playLaunchContext.getTenant();

        Object accountId = checkAndGet(account, InterfaceName.AccountId.name());
        if (accountId == null) {
            throw new RuntimeException("Account Id can not be null");
        }

        Recommendation recommendation = new Recommendation();
        recommendation.setRecommendationId(UUID.randomUUID().toString());
        recommendation.setDescription(playLaunch.getPlay().getDescription());
        recommendation.setLaunchId(playLaunchId);
        recommendation.setPlayId(playName);

        Date launchTime = playLaunch.getCreated();
        if (launchTime == null) {
            launchTime = new Date(launchTimestampMillis);
        }
        recommendation.setLaunchDate(launchTime);

        recommendation.setAccountId(accountId.toString());
        recommendation.setLeAccountExternalID(accountId.toString());
        recommendation.setSfdcAccountID(checkAndGet(account, InterfaceName.SalesforceAccountID.name()));
        Double value = 0D;
        recommendation.setMonetaryValue(value);

        // give preference to lattice data cloud field LDC_Name. If not found
        // then try to get company name from customer data itself.
        recommendation.setCompanyName(checkAndGet(account, InterfaceName.LDC_Name.name()));
        if (recommendation.getCompanyName() == null) {
            recommendation.setCompanyName(checkAndGet(account, InterfaceName.CompanyName.name()));
        }

        recommendation.setTenantId(tenant.getPid());
        recommendation.setLikelihood(DEFAULT_LIKELIHOOD);
        recommendation.setSynchronizationDestination(PlaymakerConstants.SFDC);

        recommendation.setPriorityID(bucket);
        recommendation.setPriorityDisplayName(bucket.getName());

        if (mapForAccountAndContactList.containsKey(accountId)) {
            List<Map<String, String>> contactsForRecommendation = PlaymakerUtils
                    .generateContactForRecommendation(mapForAccountAndContactList.get(accountId));
            recommendation.setExpandedContacts(contactsForRecommendation);
        }

        return recommendation;
    }

    private String checkAndGet(Map<String, Object> account, String columnName) {
        return account.get(columnName) != null ? account.get(columnName).toString() : null;
    }
}
