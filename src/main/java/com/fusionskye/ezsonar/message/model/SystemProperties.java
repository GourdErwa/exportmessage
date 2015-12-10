package com.fusionskye.ezsonar.message.model;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Properties;

/**
 * @author wei.Li by 15/12/7
 */
public class SystemProperties {

    private String

            log4j2RunModel,

    elasticsearchUrl,
            elasticsearchCluster,
            elasticsearchIndexPrefix,
            elasticsearchIndexType,


    mongodbUseAuth,
            mongodbUser,
            mongodbPassword,
            mongodbHost,
            mongodbDatabase,
            mongodbPort,


    exportCVSFilePath,
            exportCVSFileEncoding,
            isShowTableHead,

    calculationSecondsAhead,
            statisticalFieldValues;


    private SystemProperties(String log4j2RunModel,
                             String elasticsearchUrl, String elasticsearchCluster, String elasticsearchIndexPrefix, String elasticsearchIndexType,
                             String mongodbUseAuth, String mongodbUser, String mongodbPassword, String mongodbHost, String mongodbDatabase, String mongodbPort,
                             String exportCVSFilePath, String exportCVSFileEncoding, String isShowTableHead,
                             String calculationSecondsAhead, String statisticalFieldValues) {
        this.log4j2RunModel = log4j2RunModel;
        this.elasticsearchUrl = elasticsearchUrl;
        this.elasticsearchCluster = elasticsearchCluster;
        this.elasticsearchIndexPrefix = elasticsearchIndexPrefix;
        this.elasticsearchIndexType = elasticsearchIndexType;
        this.mongodbUseAuth = mongodbUseAuth;
        this.mongodbUser = mongodbUser;
        this.mongodbPassword = mongodbPassword;
        this.mongodbHost = mongodbHost;
        this.mongodbDatabase = mongodbDatabase;
        this.mongodbPort = mongodbPort;
        this.exportCVSFilePath = exportCVSFilePath;
        this.exportCVSFileEncoding = exportCVSFileEncoding;
        this.isShowTableHead = isShowTableHead;
        this.calculationSecondsAhead = calculationSecondsAhead;
        this.statisticalFieldValues = statisticalFieldValues;
    }

    /**
     * 自动装载 properties 内容到对象中
     * 默认读取类下所有字段名称存入数组,传入构造函数中
     *
     * @param properties properties
     * @return SystemProperties
     * @throws Exception
     */
    public static SystemProperties createSystemProperties(Properties properties)
            throws Exception {

        final Field[] declaredFields = SystemProperties.class.getDeclaredFields();
        final int i1 = declaredFields.length;
        final String[] values = new String[i1];
        for (int i = 0; i < i1; i++) {
            final Field declaredField = declaredFields[i];
            final String key = declaredField.getName();
            final String s = properties.getProperty(key);
            Preconditions.checkNotNull(s, key + " 未配置");
            values[i] = s;
        }
        final Constructor<?>[] constructors = SystemProperties.class.getDeclaredConstructors();
        return (SystemProperties) constructors[0].newInstance(values);
    }


    public String getLog4j2RunModel() {
        return log4j2RunModel;
    }

    public String getElasticsearchUrl() {
        return elasticsearchUrl;
    }

    public String getElasticsearchCluster() {
        return elasticsearchCluster;
    }

    public String getElasticsearchIndexPrefix() {
        return elasticsearchIndexPrefix;
    }

    public String getElasticsearchIndexType() {
        return elasticsearchIndexType;
    }

    public String getMongodbUseAuth() {
        return mongodbUseAuth;
    }

    public String getMongodbUser() {
        return mongodbUser;
    }

    public String getMongodbPassword() {
        return mongodbPassword;
    }

    public String getMongodbHost() {
        return mongodbHost;
    }

    public String getMongodbDatabase() {
        return mongodbDatabase;
    }

    public String getMongodbPort() {
        return mongodbPort;
    }

    public String getExportCVSFilePath() {
        return exportCVSFilePath;
    }

    public String getExportCVSFileEncoding() {
        return exportCVSFileEncoding;
    }

    public String getIsShowTableHead() {
        return isShowTableHead;
    }

    public String getCalculationSecondsAhead() {
        return calculationSecondsAhead;
    }

    public String getStatisticalFieldValues() {
        return statisticalFieldValues;
    }


    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("log4j2RunModel", log4j2RunModel)
                .add("elasticsearchUrl", elasticsearchUrl)
                .add("elasticsearchCluster", elasticsearchCluster)
                .add("elasticsearchIndexPrefix", elasticsearchIndexPrefix)
                .add("elasticsearchIndexType", elasticsearchIndexType)
                .add("mongodbUseAuth", mongodbUseAuth)
                .add("mongodbUser", mongodbUser)
                .add("mongodbPassword", mongodbPassword)
                .add("mongodbHost", mongodbHost)
                .add("mongodbDatabase", mongodbDatabase)
                .add("mongodbPort", mongodbPort)
                .add("exportCVSFilePath", exportCVSFilePath)
                .add("exportCVSFileEncoding", exportCVSFileEncoding)
                .add("isShowTableHead", isShowTableHead)
                .add("calculationSecondsAhead", calculationSecondsAhead)
                .add("statisticalFieldValues", statisticalFieldValues)
                .toString();
    }
}
