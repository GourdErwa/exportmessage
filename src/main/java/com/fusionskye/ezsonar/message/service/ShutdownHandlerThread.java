package com.fusionskye.ezsonar.message.service;

import com.fusionskye.ezsonar.message.dao.ESClient;
import com.fusionskye.ezsonar.message.dao.MongoDBClient;

/**
 * 系统停止运行回调函数
 */
public class ShutdownHandlerThread extends Thread {

    private CoreService coreService;

    public ShutdownHandlerThread(CoreService coreService) {
        this.coreService = coreService;
    }

    @Override
    public void run() {

        MongoDBClient.closeDB();
        ESClient.closeClient();
        if (coreService != null) {
            this.coreService.closeTimerTask();
        }

    }

}