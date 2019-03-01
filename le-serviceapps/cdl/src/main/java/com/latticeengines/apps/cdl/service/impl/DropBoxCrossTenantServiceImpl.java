package com.latticeengines.apps.cdl.service.impl;

import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.services.identitymanagement.model.AccessKeyMetadata;
import com.latticeengines.apps.cdl.entitymgr.DropBoxEntityMgr;
import com.latticeengines.apps.cdl.service.DropBoxCrossTenantService;
import com.latticeengines.aws.iam.IAMService;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.DropBox;
import com.latticeengines.domain.exposed.cdl.DropBoxAccessMode;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.service.TenantService;

@Component("DropBoxCrossTenantService")
public class DropBoxCrossTenantServiceImpl implements DropBoxCrossTenantService {


    private static final Logger log = LoggerFactory.getLogger(DropBoxCrossTenantServiceImpl.class);

    private static final String DROP_FOLDER = "dropfolder";
    private static final String POLICY_NAME = "dropfolder";
    private static final String SLASH = "/";
    // naming convention for S3 bucket policy statement
    private static final String PUT_POLICY_ID = "RequirementsOnPut";

    @Inject
    private DropBoxEntityMgr entityMgr;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private S3Service s3Service;

    @Inject
    private IAMService iamService;

    @Inject
    private TenantService tenantService;

    @Value("${aws.customer.s3.bucket}")
    private String customersBucket;

    @Value("${aws.customer.s3.region}")
    private String region;

    @Value("${aws.customer.account.id}")
    private String customerAccountId;

    @Override
    public DropBox create(String customerSpace) {
        Tenant tenant = tenantService.findByTenantName(CustomerSpace.parse(customerSpace).getTenantId());
        DropBox dropbox = entityMgr.createDropBox(tenant, region);
        String prefix = toPrefix(dropbox);
        if (!s3Service.isNonEmptyDirectory(customersBucket, prefix)) {
            s3Service.createFolder(customersBucket, prefix);
        }
        return dropbox;
    }

    @Override
    public void delete(String customerSpace) {
        Tenant tenant = tenantService.findByTenantName(CustomerSpace.parse(customerSpace).getTenantId());
        DropBox dropbox = entityMgr.getDropBox(tenant);
        if (dropbox != null) {
            String prefix = toPrefix(dropbox);
            s3Service.cleanupPrefix(customersBucket, prefix);

            String dropBoxId = dropbox.getDropBox();
            String userName = "c-" + dropBoxId;
            revokeAccessToLatticeUser(customerSpace, userName);
            String policyDoc = iamService.getUserPolicy(userName, POLICY_NAME);
            if (StringUtils.isBlank(policyDoc)) {
                iamService.deleteCustomerUser(userName);
            }
            revokeDropBoxFromBucket(dropBoxId, dropbox.getExternalAccount());
        }
        entityMgr.delete(dropbox);
    }

    @Override
    public String getDropBoxBucket() {
        return customersBucket;
    }

    @Override
    public String getDropBoxPrefix(String customerSpace) {
        Tenant tenant = tenantService.findByTenantName(CustomerSpace.parse(customerSpace).getTenantId());
        DropBox dropbox = entityMgr.getDropBox(tenant);
        if (dropbox == null) {
            return null;
        } else {
            return toPrefix(dropbox);
        }
    }

    private void revokeAccessToLatticeUser(String customerSpace, String userName) {
        String prefix = getDropBoxPrefix(customerSpace);
        String dropBoxId = prefix.substring(prefix.indexOf(SLASH) + 1);
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
                .filter(stmt -> !stmt.getId().contains(dropboxId) //
                        && CollectionUtils.isNotEmpty(stmt.getResources())) //
                .collect(Collectors.toList());
        policy.setStatements(nonEmptyStmts);
    }


    private void revokeDropBoxFromBucket(String dropBoxId, String accountId) {
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
        if (StringUtils.isNotBlank(accountId) && CollectionUtils.isNotEmpty(nonEmptyStmts) //
                && !accountId.contains(customerAccountId)) {
            boolean accountIsRedundant = nonEmptyStmts.stream() //
                    .noneMatch(stmt -> accountId.equals(stmt.getId()));
            if (accountIsRedundant) {
                nonEmptyStmts = nonEmptyStmts.stream().peek(stmt -> {
                    if (PUT_POLICY_ID.equals(stmt.getId())) {
                        removeAccountFromPutStatement(stmt, accountId);
                    }
                }).collect(Collectors.toList());
            }
        }
        policy.setStatements(nonEmptyStmts);
        if (CollectionUtils.isEmpty(nonEmptyStmts)) {
            s3Service.deleteBucketPolicy(customersBucket);
        } else {
            s3Service.setBucketPolicy(customersBucket, policy.toJson());
        }
    }

    @Override
    public DropBoxSummary getDropBoxSummary(String customerSpace) {
        Tenant tenant = tenantService.findByTenantName(CustomerSpace.parse(customerSpace).getTenantId());
        DropBox dropbox = entityMgr.getDropBox(tenant);
        if (dropbox == null) {
            return null;
        } else {
            DropBoxSummary summary = new DropBoxSummary();
            summary.setBucket(customersBucket);
            summary.setDropBox(dropbox.getDropBox());
            if (dropbox.getAccessMode() != null) {
                summary.setAccessMode(dropbox.getAccessMode());
                if (DropBoxAccessMode.LatticeUser.equals(dropbox.getAccessMode())) {
                    summary.setLatticeUser(dropbox.getLatticeUser());
                    String userName = dropbox.getLatticeUser();
                    AccessKeyMetadata md = iamService.getCustomerKeyIfExists(userName);
                    if (md != null) {
                        summary.setAccessKeyId(md.getAccessKeyId());
                    }
                } else {
                    summary.setExternalAccount(dropbox.getExternalAccount());
                }
            }
            return summary;
        }
    }

    @Override
    public Tenant getDropBoxOwner(String dropBox) {
        return entityMgr.getDropBoxOwner(dropBox);
    }

    private void removeAccountFromPutStatement(Statement statement, String accountId) {
        List<Principal> principals = statement.getPrincipals();
        principals = principals.stream() //
                .filter(principal -> !principal.getId().contains(accountId)).collect(Collectors.toList());
        statement.setPrincipals(principals);
    }

    private String toPrefix(DropBox dropbox) {
        return DROP_FOLDER + SLASH + dropbox.getDropBox();
    }
}
