package com.fusionskye.ezsonar.message.service;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * ES debug 打印记录
 *
 * @author wei.Li by 15/4/2 (gourderwa@163.com).
 */
public class MessageGatewayHelper {

    private static final Logger LOG = LoggerFactory.getLogger(MessageGatewayHelper.class);

    /**
     * 日志打印 SearchResponse
     *
     * @param searchBuilder  searchBuilder
     * @param searchResponse searchResponse
     */
    protected static void debugSearchResponse(SearchRequestBuilder searchBuilder, SearchResponse searchResponse) {

        if (searchResponse == null) {

            String indices = "indices: " + Arrays.asList(searchBuilder.request().indices()).toString() + " ";
            String responseString =
                    "searchResponse is null";
            String toString = searchBuilder.internalBuilder().toString();
            LOG.error(toString + indices + responseString);
            return;

        }

        if (LOG.isDebugEnabled()) {
            String indices = "indices: " + Arrays.asList(searchBuilder.request().indices()).toString() + " ";

            String responseString =
                    "Status: " + searchResponse.status() + ", TookInMillis: " + searchResponse.getTookInMillis() +
                            ", Hits: " + searchResponse.getHits().getTotalHits();

            String toString = searchBuilder.internalBuilder().toString();
            LOG.debug(toString + indices + responseString);
        }
    }
}
