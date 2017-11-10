package com.latticeengines.oauth2db.exposed.util;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.security.oauth2.provider.OAuth2Request;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class OAuth2AuthenticationDeserializer extends JsonDeserializer<OAuth2Authentication> {

	private static final Logger log = LoggerFactory.getLogger(OAuth2AuthenticationDeserializer.class);
	
	private static final String STRING_APPROVED = "approved";
	private static final String STRING_AUTHENTICATED = "authenticated";
	private static final String STRING_AUTHORITIES = "authorities";
	private static final String STRING_AUTHORITY = "authority";
	private static final String STRING_CLIENT_ID = "clientId";
	private static final String STRING_CREDENTIALS = "credentials";
	private static final String STRING_OAUTH2_REQUEST = "oauth2Request";
	private static final String STRING_PRINCIPAL = "principal";
	private static final String STRING_REDIRECT_URI = "redirectUri";
	private static final String STRING_REQUEST_PARAMETERS = "requestParameters";
	private static final String STRING_RESOURCE_IDS = "resourceIds";
	private static final String STRING_RESPONSE_TYPES = "responseTypes";
	private static final String STRING_SCOPE = "scope";
	private static final String STRING_USER_AUTHENTICATION = "userAuthentication";

	@Override
	public OAuth2Authentication deserialize(JsonParser jp, DeserializationContext ctxt)
			throws IOException, JsonProcessingException {
		System.out.println("Init Custom deserialization");
		ObjectCodec oc = jp.getCodec();
		JsonNode node = oc.readTree(jp);
		log.info("JSON Object: " + node);

		OAuth2Request oauth2Request = extractOAuth2Request(jp, node.get(STRING_OAUTH2_REQUEST));
		Authentication userAuthentication = extractAuthentication(jp, node.get(STRING_USER_AUTHENTICATION));

		return new OAuth2Authentication(oauth2Request, userAuthentication);
	}

	private OAuth2Request extractOAuth2Request(JsonParser jp, JsonNode oauth2RequestNode) throws IOException {
		if (oauth2RequestNode == null || oauth2RequestNode.getNodeType() == JsonNodeType.NULL
				|| StringUtils.isBlank(oauth2RequestNode.toString())) {
			return null;
		}
		log.info(STRING_OAUTH2_REQUEST + " : " + oauth2RequestNode.toString());

		ObjectNode node = (ObjectNode) oauth2RequestNode;
		boolean approved = node.get(STRING_APPROVED).booleanValue();
		Set<GrantedAuthority> authorities = Sets.newHashSet(arrayToGrantedAuthorities(node.get(STRING_AUTHORITIES)));
		String clientId = getText(node, STRING_CLIENT_ID);
		String redirectUri = getText(node, STRING_REDIRECT_URI);
		Map<String, String> requestParameters = arrayToStringMap(node.get(STRING_REQUEST_PARAMETERS));
		Set<String> resourceIds = Sets.newHashSet(arrayToStrings(node.get(STRING_RESOURCE_IDS)));
		Set<String> responseTypes = Sets.newHashSet(arrayToStrings(node.get(STRING_RESPONSE_TYPES)));
		Set<String> scope = Sets.newHashSet(arrayToStrings(node.get(STRING_SCOPE)));

		OAuth2Request oauth2Request = new OAuth2Request(requestParameters, clientId, authorities, approved, scope,
				resourceIds, redirectUri, responseTypes, null);

		log.info("ClientId: " + oauth2Request.getClientId());

		return oauth2Request;
	}

	private Authentication extractAuthentication(JsonParser jp, JsonNode authNode) {
		Authentication authentication = jsonToUsernamePasswordAuthentication(authNode);
		log.info("Authentication Autorities: " + authentication.getAuthorities());
		return authentication;
	}

	private static Map<String, String> arrayToStringMap(JsonNode node) {
		if (node == null || node.getNodeType() == JsonNodeType.NULL) {
			return null;
		}
		Map<String, String> map = Maps.newHashMapWithExpectedSize(node.size());
		node.fieldNames().forEachRemaining(key -> {
			String value = getText((ObjectNode) node, key);
			if (value != null) {
				map.put(key, value);
			}
		});
		return map;
	}

	private static List<String> arrayToStrings(JsonNode node) {
		if (node == null || node.getNodeType() == JsonNodeType.NULL) {
			return null;
		}
		List<String> list = Lists.newArrayListWithCapacity(node.size());
		node.elements().forEachRemaining(e -> list.add(e.asText()));
		return list;
	}

	private static String getText(ObjectNode node, String fieldName) {
		JsonNode textNode = node.get(fieldName);
		return textNode == null || textNode.getNodeType() == JsonNodeType.NULL || StringUtils.isBlank(textNode.toString()) ? null : textNode.asText();
	}

	private static List<GrantedAuthority> arrayToGrantedAuthorities(JsonNode node) {
		if (node == null || node.getNodeType() == JsonNodeType.NULL) {
			return null;
		}
		List<GrantedAuthority> list = Lists.newArrayListWithCapacity(node.size());
		node.elements().forEachRemaining(e -> list
				.add(new SimpleGrantedAuthority(e.isTextual() ? e.asText() : e.get(STRING_AUTHORITY).asText())));
		return list;
	}

	public static UsernamePasswordAuthenticationToken jsonToUsernamePasswordAuthentication(JsonNode json) {
		if (json == null || json.getNodeType() == JsonNodeType.NULL) {
			return null;
		}
		ObjectNode node = (ObjectNode) json;
		String principal = getText(node, STRING_PRINCIPAL);
		String credentials = getText(node, STRING_CREDENTIALS);
		List<GrantedAuthority> authorities = arrayToGrantedAuthorities(node.get(STRING_AUTHORITIES));
		UsernamePasswordAuthenticationToken token;
		if (node.get(STRING_AUTHENTICATED).asBoolean()) {
			token = new UsernamePasswordAuthenticationToken(principal, credentials, authorities);
		} else {
			token = new UsernamePasswordAuthenticationToken(principal, credentials);
		}
		return token;
	}

	/*
	public static UserDetails jsonToUserDetails(JsonNode json) {
		if (json == null || json.getNodeType() == JsonNodeType.NULL) {
			return null;
		}
		ObjectNode node = (ObjectNode) json;
		UserDetails userDetails = new User(getText(node, STRING_USERNAME), getText(node, STRING_PASSWORD),
				node.get(STRING_ENABLED).booleanValue(), node.get(STRING_ACCOUNT_NON_EXPIRED).booleanValue(),
				node.get(STRING_CREDENTIALS_NON_EXPIRED).booleanValue(),
				node.get(STRING_ACCOUNT_NON_LOCKED).booleanValue(),
				arrayToGrantedAuthorities(node.get(STRING_AUTHORITIES)));
		return userDetails;
	}
	*/
}
