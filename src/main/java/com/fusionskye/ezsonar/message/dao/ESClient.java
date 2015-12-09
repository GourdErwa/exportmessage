package com.fusionskye.ezsonar.message.dao;

import com.fusionskye.ezsonar.message.model.SystemProperties;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.URI;

/**
 * @author wei.Li by 15/12/7
 */
public class ESClient {

    private static Client client;

    /**
     * @param sp 系统配置封装对象
     * @throws Exception
     */
    public static synchronized void createESClient(SystemProperties sp)
            throws Exception {
        if (client != null) {
            return;
        }
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("client.transport.sniff", false)
                .put("cluster.name", sp.getElasticsearchCluster())
                .build();
        URI uri = new URI(sp.getElasticsearchUrl());
        String host = uri.getHost();
        int port = uri.getPort();
        if (port == 9200) {
            port = 9300;
        }
        client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(host, port));
    }

    public static Client getClient() {
        return client;
    }

    public static void closeClient() {

        if (client != null) {
            client.close();
        }
    }

}
