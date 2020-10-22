package com.latticeengines.oauth2.authserver;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.oauth.OAuthUser;
import com.latticeengines.oauth2db.exposed.entitymgr.OAuthUserEntityMgr;

/**
 * The AuthenticationManager used for the oauth password flow. This provides
 * One-Time Key behavior.
 */
@Component
public class OneTimeKeyAuthenticationManager implements AuthenticationManager {

    private static final Set<String> oneTimePasswordUsers = ImmutableSet.of("lp", "playmaker");

    private static final Logger log = LoggerFactory.getLogger(OneTimeKeyAuthenticationManager.class);

    @Inject
    private OAuthUserEntityMgr users;

    @Inject
    private OrgIdRegister orgIdRegister;

    private PasswordEncoder encoder = new BCryptPasswordEncoder();

    public OneTimeKeyAuthenticationManager() {
    }

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {

        String password = (String) authentication.getCredentials();
        if (password == null) {
            throw new BadCredentialsException("Missing credentials");
        }
        OAuthUser user = users.get(authentication.getName());
        if (user == null) {
            throw new BadCredentialsException("Invalid credentials");
        }
        log.info("user=" + user.getUserId());

        if (oneTimePasswordUsers.contains(authentication.getName().toLowerCase())) {
            DateTime now = DateTime.now(DateTimeZone.UTC);
            boolean expired = user.getPasswordExpired()
                    || (user.getPasswordExpiration() != null && now.isAfter(new DateTime(user.getPasswordExpiration())));
            if (expired) {
                throw new BadCredentialsException(
                        "The provided password/one-time key has expired. Please request a new one.");
            } else if (!encoder.matches(password, user.getEncryptedPassword())) {
                throw new BadCredentialsException("Invalid password/one-time key provided");
            }

            user.setPasswordExpired(true);
            users.update(user);
        }

        List<SimpleGrantedAuthority> authorities = new ArrayList<>();
        if (oneTimePasswordUsers.contains(authentication.getName().toLowerCase())) {
            authorities.add(new SimpleGrantedAuthority("ROLE_PLAYMAKER_ADMIN"));
            orgIdRegister.registerOrgId(authentication.getName());
        } else {
            authentication.getAuthorities().forEach(auth -> {
                log.info("auth=" + auth.toString());
                authorities.add(new SimpleGrantedAuthority(auth.toString()));
            });
        }

        return new UsernamePasswordAuthenticationToken(authentication.getName(), password, authorities);
    }
}
