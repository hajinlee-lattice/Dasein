package com.latticeengines.oauth2.authserver;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.oauth.OAuthUser;
import com.latticeengines.oauth2.common.entitymgr.OAuthUserEntityMgr;

@Component
public class LatticeAuthenticationManager implements AuthenticationManager {

    @Autowired
    private OAuthUserEntityMgr users;

    private PasswordEncoder encoder = new BCryptPasswordEncoder();

    public LatticeAuthenticationManager() {
    }

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        String password = (String) authentication.getCredentials();
        if (password == null) {
            throw new BadCredentialsException("Missing credentials");
        }
        OAuthUser user = users.get(authentication.getName());

        if (user == null) {
            throw new BadCredentialsException(String.format("Unknown user %s provided", authentication.getName()));
        }

        DateTime now = DateTime.now(DateTimeZone.UTC);
        if (user.getPasswordExpiration() != null && now.isAfter(new DateTime(user.getPasswordExpiration()))) {
            throw new BadCredentialsException("The password has expired");
        } else if (!encoder.matches(password, user.getPassword())) {
            throw new BadCredentialsException("Invalid password provided");
        }

        List<SimpleGrantedAuthority> authorities = new ArrayList<SimpleGrantedAuthority>();
        authorities.add(new SimpleGrantedAuthority("ROLE_PLAYMAKER_ADMIN"));

        return new UsernamePasswordAuthenticationToken(authentication.getName(), password, authorities);
    }
}
