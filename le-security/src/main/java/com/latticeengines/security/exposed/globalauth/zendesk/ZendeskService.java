package com.latticeengines.security.exposed.globalauth.zendesk;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.zendesk.ZendeskUser;

/**
 * Zendesk client
 * {@link LedpException} will be thrown with {@link LedpCode#LEDP_00005} if Zendesk API request limit is reached
 */
public interface ZendeskService {

    /**
     * Upsert Zendesk user, properties will be merged into current user (if exists)
     *
     * @param user user object, must contains either email or ID and name
     * @return entire user object after the operation
     */
    ZendeskUser createOrUpdateUser(ZendeskUser user);

    /**
     * Partially update Zendesk user (must exists) with the given object.
     *
     * @param user user object, must contains ID
     * @throws LedpException with {@link LedpCode#LEDP_19003} if the user does not exist
     * @return entire user object after the operation
     */
    ZendeskUser updateUser(ZendeskUser user);

    /**
     * Find Zendesk user by email
     *
     * @param email email used to search Zendesk user
     * @return corresponding user object, or null if the user does not exist
     */
    ZendeskUser findUserByEmail(String email);

    /**
     * Suspend access of the Zendesk user with the specified email
     *
     * @param userId ID of the user to suspend
     * @throws LedpException with {@link LedpCode#LEDP_19003} if the user does not exist
     */
    void suspendUser(long userId);

    /**
     * Resume access of the Zendesk user with the specified email
     *
     * @param userId ID of the user to resume
     * @throws LedpException with {@link LedpCode#LEDP_19003} if the user does not exist
     */
    void unsuspendUser(long userId);

    /**
     * Suspend access of the Zendesk user with the specified email, noop if no user contains the email
     *
     * @param email email of the user to suspend
     */
    void suspendUserByEmail(String email);

    /**
     * Resume access of the Zendesk user with the specified email, noop if no user contains the email
     *
     * @param email email of the user to resume
     */
    void unsuspendUserByEmail(String email);

    /**
     * Set the password of the Zendesk user with the specified ID
     *
     * @param userId   ID of the target user
     * @param password new password
     * @throws LedpException with {@link LedpCode#LEDP_19003} if the user does not exist
     */
    void setUserPassword(long userId, String password);
}
