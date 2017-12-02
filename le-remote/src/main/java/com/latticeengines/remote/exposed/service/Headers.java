package com.latticeengines.remote.exposed.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.http.message.BasicNameValuePair;

import com.google.common.collect.ImmutableList;

public class Headers {

    private static ImmutableList<BasicNameValuePair> headers = null;

    static {
        List<BasicNameValuePair> list = new ArrayList<>();
        list.add(new BasicNameValuePair("MagicAuthentication", "Security through obscurity!"));
        list.add(new BasicNameValuePair("Accept", "application/json"));
        headers = ImmutableList.copyOf(list);
    }

    public static List<BasicNameValuePair> getHeaders() {
        return headers;
    }

}
