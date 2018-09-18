package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.actions.S3Actions;
import com.amazonaws.auth.policy.conditions.StringCondition;
import com.amazonaws.services.identitymanagement.model.AccessKey;
import com.latticeengines.apps.cdl.entitymgr.DropBoxEntityMgr;
import com.latticeengines.apps.cdl.service.DropBoxService;
import com.latticeengines.aws.iam.IAMService;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.DropBox;
import com.latticeengines.domain.exposed.cdl.DropBoxAccessMode;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessRequest;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessResponse;

@Service("dropBoxService")
public class DropBoxServiceImpl implements DropBoxService {

    private static final Logger log = LoggerFactory.getLogger(DropBoxServiceImpl.class);

    private static final String DROPBOX = "dropbox";
    private static final String POLICY_NAME = "dropbox";
    private static final String WILD_CARD = "/*";
    private static final String ARN_PREFIX = "arn:aws:s3:::";

    @Inject
    private DropBoxEntityMgr entityMgr;

    @Inject
    private S3Service s3Service;

    @Inject
    private IAMService iamService;

    @Value("${aws.customer.s3.bucket}")
    private String customersBucket;

    @Override
    public DropBox create() {
        DropBox dropbox = entityMgr.createDropBox();
        String prefix = toPrefix(dropbox);
        if (!s3Service.isNonEmptyDirectory(customersBucket, prefix)) {
            s3Service.createFolder(customersBucket, prefix);
        }
        return dropbox;
    }

    @Override
    public void delete() {
        DropBox dropbox = entityMgr.getDropBox();
        if (dropbox != null) {
            String prefix = toPrefix(dropbox);
            s3Service.cleanupPrefix(customersBucket, prefix);

            prefix = getDropBoxPrefix();
            String dropBoxId = prefix.substring(prefix.indexOf("/") + 1);
            String userName = "c-" + dropBoxId;
            revokeAccessToLatticeUser(userName);
            String policyDoc = iamService.getUserPolicy(userName, POLICY_NAME);
            if (StringUtils.isBlank(policyDoc)) {
                iamService.deleteCustomerUser(userName);
            }
            revokeDropBoxFromBucket(dropBoxId);
        }
        entityMgr.delete(dropbox);
    }

    @Override
    public String getDropBoxBucket() {
        return customersBucket;
    }

    @Override
    public String getDropBoxPrefix() {
        DropBox dropbox = entityMgr.getDropBox();
        if (dropbox == null) {
            return null;
        } else {
            return toPrefix(dropbox);
        }
    }

    @Override
    public DropBoxSummary getDropBoxSummary() {
        DropBox dropbox = entityMgr.getDropBox();
        if (dropbox == null) {
            return null;
        } else {
            DropBoxSummary summary = new DropBoxSummary();
            summary.setBucket(customersBucket);
            summary.setDropBox(dropbox.getDropBox());
            if (dropbox.getAccessMode() != null) {
                summary.setAccessMode(dropbox.getAccessMode());
                summary.setExternalAccount(dropbox.getExternalAccount());
                summary.setLatticeUser(dropbox.getLatticeUser());
            }
            return summary;
        }
    }

    @Override
    public GrantDropBoxAccessResponse grantAccess(GrantDropBoxAccessRequest request) {
        DropBox dropbox = entityMgr.getDropBox();
        if (dropbox != null) {
            revokeAccess();
            dropbox.setAccessMode(request.getAccessMode());
            GrantDropBoxAccessResponse response;
            switch (request.getAccessMode()) {
                case LatticeUser:
                    response = grantAccessToLatticeUser(request.getExistingUser());
                    dropbox.setLatticeUser(response.getLatticeUser());
                    break;
                case ExternalAccount:
                    response = grantAccessToExternalAccount(request.getExternalAccountId());
                    dropbox.setExternalAccount(response.getExternalAccountId());
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown access mode " + request.getAccessMode());
            }
            entityMgr.update(dropbox);
            return response;
        } else {
            throw new RuntimeException("Tenant " + MultiTenantContext.getShortTenantId() + " does not have a dropbox.");
        }
    }

    @Override
    public void revokeAccess() {
        DropBox dropbox = entityMgr.getDropBox();
        if (dropbox != null && dropbox.getAccessMode() != null) {
            switch (dropbox.getAccessMode()) {
                case LatticeUser:
                    revokeAccessToLatticeUser(dropbox.getLatticeUser());
                    break;
                case ExternalAccount:
                    revokeDropBoxFromBucket(getDropBoxId());
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown access mode " + dropbox.getAccessMode());
            }
        }
    }

    private String getDropBoxId() {
        String prefix = getDropBoxPrefix();
        return prefix.substring(prefix.indexOf("/") + 1);
    }

    private GrantDropBoxAccessResponse grantAccessToLatticeUser(String existingUser) {
        String bucket = getDropBoxBucket();
        String prefix = getDropBoxPrefix();
        String dropBoxId = prefix.substring(prefix.indexOf("/") + 1);
        String userName = existingUser;
        if (StringUtils.isBlank(userName)) {
            userName = "c-" + dropBoxId;
            iamService.createCustomerUser(userName);
        }
        AccessKey accessKey = null;
        if (!iamService.hasCustomerKey(userName)) {
            accessKey = iamService.createCustomerKey(userName);
        }
        upsertDropBoxPolicy(userName, bucket, dropBoxId);
        GrantDropBoxAccessResponse response = new GrantDropBoxAccessResponse();
        response.setAccessMode(DropBoxAccessMode.LatticeUser);
        response.setLatticeUser(userName);
        if (accessKey != null) {
            response.setAccessKey(accessKey.getAccessKeyId());
            response.setSecretKey(accessKey.getSecretAccessKey());
        }
        return response;
    }

    private void upsertDropBoxPolicy(String userName, String bucket, String dropBoxId) {
        String policyDoc = iamService.getUserPolicy(userName, POLICY_NAME);
        Policy policy;
        if (StringUtils.isBlank(policyDoc)) {
            policy = newDropBoxPolicy(bucket, dropBoxId);
        } else {
            policy = Policy.fromJson(policyDoc);
            appendDropBox(policy, bucket, dropBoxId);
        }
        iamService.putUserPolicy(userName, POLICY_NAME, policy.toJson());
    }

    private Policy newDropBoxPolicy(String bucket, String dropBoxId) {
        String arnPrefix = ARN_PREFIX + bucket + "/" + DROPBOX + "/" + dropBoxId;
        return new Policy().withStatements(//
                listDropBoxStatement(bucket, dropBoxId), //
                new Statement(Statement.Effect.Allow) //
                        .withId("Objects") //
                        .withActions(//
                                S3Actions.AbortMultipartUpload, //
                                S3Actions.GetObject, //
                                S3Actions.PutObject, //
                                S3Actions.DeleteObject, //
                                S3Actions.SetObjectAcl //
                        ) //
                        .withResources(new Resource(arnPrefix + WILD_CARD)) //
        );
    }

    private void appendDropBox(Policy policy, String bucket, String dropBoxId) {
        boolean hasListBucketStmt = false;
        String listStmtId = listDropBoxStmtId(dropBoxId);
        for (Statement stmt: policy.getStatements()) {
            if (listStmtId.equals(stmt.getId())) {
                hasListBucketStmt = true;
                break;
            }
        }
        policy.getStatements().forEach(stmt -> {
            if ("Objects".equals(stmt.getId())) {
                List<Resource> resourceList = stmt.getResources();
                Set<String> resources = resourceList.stream().map(Resource::getId)
                        .collect(Collectors.toSet());
                String rsc = ARN_PREFIX + bucket + "/" + DROPBOX + "/" + dropBoxId + WILD_CARD;
                if (!resources.contains(rsc)) {
                    resourceList.add(new Resource(rsc));
                    resources.add(rsc);
                }
            }
        });
        if (!hasListBucketStmt) {
            policy.getStatements().add(listDropBoxStatement(bucket, dropBoxId));
        }
    }

    private Statement listDropBoxStatement(String bucket, String dropBoxId) {
        return new Statement(Statement.Effect.Allow) //
                .withId(listDropBoxStmtId(dropBoxId)) //
                .withActions(S3Actions.ListObjects) //
                .withResources(new Resource(ARN_PREFIX + bucket)) //
                .withConditions(new StringCondition(//
                        StringCondition.StringComparisonType.StringLike, //
                        "s3:prefix", //
                        DROPBOX + "/" + dropBoxId + "*" //
                ));
    }

    private String listDropBoxStmtId(String dropBoxId) {
        return "List" + dropBoxId;
    }

    private void revokeAccessToLatticeUser(String userName) {
        String prefix = getDropBoxPrefix();
        String dropBoxId = prefix.substring(prefix.indexOf("/") + 1);
        String policyDoc = iamService.getUserPolicy(userName, POLICY_NAME);
        if (StringUtils.isNotBlank(policyDoc)) {
            Policy policy = Policy.fromJson(policyDoc);
            removeDropBoxFromUser(policy, dropBoxId);
            if (CollectionUtils.isEmpty(policy.getStatements())) {
                String msg = "After revoking access to dropbox " + dropBoxId + " from user " + userName + ", " //
                        + POLICY_NAME + " policy becomes dummy, remove it";
                log.info(msg);
                iamService.deleteUserPolicy(userName, POLICY_NAME);
            } else {
                iamService.putUserPolicy(userName, POLICY_NAME, policy.toJson());
            }
        }
    }

    private void removeDropBoxFromUser(Policy policy, String dropboxId) {
        List<Statement> nonEmptyStmts = policy.getStatements().stream() //
                .peek(stmt -> {
                    List<Resource> resourceList = stmt.getResources().stream() //
                            .filter(rsc -> !rsc.getId().contains(dropboxId))//
                            .collect(Collectors.toList());
                    stmt.setResources(resourceList);
                }) //
                .filter(stmt -> !stmt.getId().contains(dropboxId) && CollectionUtils.isNotEmpty(stmt.getResources())) //
                .collect(Collectors.toList());
        policy.setStatements(nonEmptyStmts);
    }

    private GrantDropBoxAccessResponse grantAccessToExternalAccount(String accountId) {
        if (StringUtils.isBlank(accountId)) {
            throw new IllegalArgumentException("Must provide a valid account id");
        }
        String dropBoxId = getDropBoxId();
        Policy policy = getCustomerPolicy(dropBoxId, accountId);
        s3Service.setBucketPolicy(customersBucket, policy.toJson());
        log.info("Granted access to dropbox " + dropBoxId + " to external account " + accountId);

        GrantDropBoxAccessResponse response = new GrantDropBoxAccessResponse();
        response.setAccessMode(DropBoxAccessMode.ExternalAccount);
        response.setExternalAccountId(accountId);
        return response;
    }

    private Policy getCustomerPolicy(String dropBoxId, String accountId) {
        String bucketPolicy = s3Service.getBucketPolicy(customersBucket);
        List<Statement> statements = new ArrayList<>();
        Policy policy;
        if (StringUtils.isBlank(bucketPolicy)) {
            policy = new Policy();
        } else {
            policy = Policy.fromJson(bucketPolicy);
            revokeAccountFromDropBox(policy, dropBoxId, accountId);
        }
        boolean hasAccountStmt = false;
        if (CollectionUtils.isNotEmpty(policy.getStatements())) {
            for (Statement stmt: policy.getStatements()) {
                if (stmt.getId().equals(accountId)) {
                    insertAccountStatement(customersBucket, dropBoxId, stmt);
                    hasAccountStmt = true;
                }
                statements.add(stmt);
            }
        }
        if (!hasAccountStmt) {
            statements.add(getAccountStatement(customersBucket, dropBoxId, accountId));
        }
        statements.add(getAccountListDropBoxStatement(customersBucket, dropBoxId, accountId));
        policy.setStatements(statements);
        return policy;
    }

    private Statement getAccountStatement(String bucketName, String dropBoxId, String accountId) {
        String arn = ARN_PREFIX + bucketName + "/dropbox/" + dropBoxId;
        return new Statement(Statement.Effect.Allow) //
                .withId(accountId) //
                .withPrincipals(new Principal(accountId)) //
                .withActions(//
                        S3Actions.AbortMultipartUpload, //
                        S3Actions.GetObject, //
                        S3Actions.PutObject, //
                        S3Actions.DeleteObject, //
                        S3Actions.SetObjectAcl //
                ) //
                .withResources(new Resource(arn + "*"));
    }

    private Statement getAccountListDropBoxStatement(String bucketName, String dropBoxId, String accountId) {
        return new Statement(Statement.Effect.Allow) //
                .withId(accountId + "_" + dropBoxId + "_list") //
                .withPrincipals(new Principal(accountId)) //
                .withActions(S3Actions.ListObjects) //
                .withResources(new Resource(ARN_PREFIX + bucketName))
                .withConditions(new StringCondition(//
                        StringCondition.StringComparisonType.StringLike, //
                        "s3:prefix", //
                        DROPBOX + "/" + dropBoxId + "*" //
                ));
    }

    private void revokeAccountFromDropBox(Policy policy, String dropBoxId, String accountId) {
        List<Statement> nonEmptyStmts = policy.getStatements().stream() //
                .peek(stmt -> {
                    if (accountId.equals(stmt.getId())) {
                        List<Resource> resourceList = stmt.getResources().stream() //
                                .filter(rsc -> !rsc.getId().contains(dropBoxId))//
                                .collect(Collectors.toList());
                        stmt.setResources(resourceList);
                    }
                }) //
                .filter(stmt -> {
                    boolean keep = true;
                    if (CollectionUtils.isEmpty(stmt.getResources())) {
                        keep = false;
                    } else if (stmt.getId().contains(accountId) && stmt.getId().contains(dropBoxId)) {
                        keep = false;
                    }
                    return keep;
                }) //
                .collect(Collectors.toList());
        policy.setStatements(nonEmptyStmts);
    }

    private void revokeDropBoxFromBucket(String dropBoxId) {
        String bucketPolicy = s3Service.getBucketPolicy(customersBucket);
        if (StringUtils.isBlank(bucketPolicy)) {
            return;
        }
        Policy policy = Policy.fromJson(bucketPolicy);
        List<Statement> nonEmptyStmts = policy.getStatements().stream() //
                .peek(stmt -> {
                    List<Resource> resourceList = stmt.getResources().stream() //
                            .filter(rsc -> !rsc.getId().contains(dropBoxId))//
                            .collect(Collectors.toList());
                    stmt.setResources(resourceList);
                }) //
                .filter(stmt -> {
                    boolean keep = true;
                    if (CollectionUtils.isEmpty(stmt.getResources())) {
                        keep = false;
                    } else if (stmt.getId().contains(dropBoxId)) {
                        keep = false;
                    }
                    return keep;
                }) //
                .collect(Collectors.toList());
        policy.setStatements(nonEmptyStmts);
        if (CollectionUtils.isEmpty(nonEmptyStmts)) {
            s3Service.deleteBucketPolicy(customersBucket);
        } else {
            s3Service.setBucketPolicy(customersBucket, policy.toJson());
        }
    }

    private void insertAccountStatement(String bucketName, String dropBoxId, Statement statement) {
        String arn = ARN_PREFIX + bucketName + "/dropbox/" + dropBoxId;
        List<Resource> rscs = new ArrayList<>(statement.getResources());
        rscs.add(new Resource(arn));
        rscs.add(new Resource(arn + "*"));
        statement.setResources(rscs);
    }

    private String toPrefix(DropBox dropbox) {
        return DROPBOX + "/" + dropbox.getDropBox();
    }

    // for tests
    void setCustomersBucket(String customersBucket) {
        this.customersBucket = customersBucket;
    }
}
