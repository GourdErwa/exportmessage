package com.fusionskye.ezsonar.message.dao;

import com.fusionskye.ezsonar.message.model.SystemProperties;
import com.google.common.collect.Lists;
import com.mongodb.*;

import java.util.List;

/**
 * @author wei.Li by 15/12/7
 */
public final class MongoDBClient {

    private static MongoClient mongoClient = null;
    private static String dbName = null;

    private MongoDBClient() {
    }

    /**
     * @param sp 系统配置封装对象
     * @throws Exception
     */
    public static synchronized void createMongoDBClient(SystemProperties sp)
            throws Exception {
        if (mongoClient != null) {
            return;
        }
        MongoClientOptions.Builder build = new MongoClientOptions.Builder();
        build.connectionsPerHost(3);
        build.threadsAllowedToBlockForConnectionMultiplier(3);
        build.maxWaitTime(1000 * 60 * 2);
        build.connectTimeout(1000 * 60);    //与数据库建立连接的timeout设置为1分钟

        MongoClientOptions myOptions = build.build();
        ServerAddress sa = new ServerAddress(sp.getMongodbHost(), Integer.parseInt(sp.getMongodbPort()));
        List<MongoCredential> mongoCredentialList = Lists.newArrayList();
        //是否验证用户
        if (Boolean.TRUE.toString().equalsIgnoreCase(sp.getMongodbUseAuth())) {
            mongoCredentialList.add(
                    MongoCredential.createMongoCRCredential(
                            sp.getMongodbUser(),
                            sp.getMongodbDatabase(),
                            sp.getMongodbPassword().toCharArray()
                    )
            );
        }
        mongoClient = new MongoClient(sa, mongoCredentialList, myOptions);
        dbName = sp.getMongodbDatabase();

    }

    public static DB getDB() {
        return mongoClient.getDB(dbName);
    }

    public static void closeDB() {

        if (mongoClient != null) {
            mongoClient.close();
        }
    }

}
