package com.fusionskye.ezsonar.message.service;

import com.fusionskye.ezsonar.message.model.SearchVo;
import com.fusionskye.ezsonar.message.model.SystemProperties;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * 执行定时任务,导出 cvs 文件
 *
 * @author wei.Li by 15/12/8
 */
public class CoreService {

    private static final Logger LOGGER = LoggerFactory.getLogger(CoreService.class);

    /**
     * 1分钟毫秒数
     */
    private static final long MILLISECOND_IN_MINUTE = 60 * 1000L;
    /**
     * 是否删除有空数据的行
     */
    private static final boolean DELETE_HAS_NULL_VALUE_LINE = true;

    private final Timer timer = new Timer();

    private SystemProperties systemProperties;
    private EsService esService;

    public CoreService(SystemProperties systemProperties) {
        this.systemProperties = systemProperties;
        this.esService = new EsService(this);
    }

    /**
     * 导出 cvs 文件
     * EZSonar 导出的数据存放路径如下，字母命名， 文件名使用日期&时间格式， 例如 “201512031200”
     * 文件为一分钟一个文件，目录结构：
     * 一级目录：业务路径，如***系统
     * 二级目录：逻辑节点，如***应用服务器
     * 二级目录:存放一分钟一个的文件
     *
     * @param searchVo       searchVo
     * @param outPutRootPath 导出文件主目录
     */
    private void createCSVFile(SearchVo searchVo, String outPutRootPath) {

        BufferedWriter writer = null;

        final String encoding = this.getSystemProperties().getExportCVSFileEncoding();
        final String topoName = searchVo.getTopoName();
        final String nodeName = searchVo.getNode().getName();
        final String pathname = outPutRootPath + File.separator + topoName + File.separator + nodeName;

        File csvFile = new File(pathname + File.separator + searchVo.getFileNameStartTimeFormat() + ".csv");

        try {

            Files.createParentDirs(csvFile);
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(csvFile), encoding), 1024);

            // 写入文件头部
            String headStr = "时间,节点名称,交易数量,响应数量,响应时间";

            final Set<Map.Entry<String, String>> entries = EsService.getStatisticsGroupFieldMap().entrySet();

            List<String> headStrList = Lists.newArrayList();
            List<String> fieldStrList = Lists.newArrayList();

            for (Map.Entry<String, String> entry : entries) {
                final String key = entry.getKey();
                fieldStrList.add(key);

                final String value = entry.getValue();
                headStrList.add(value);
            }


            for (String value : headStrList) {
                headStr += "," + value;
            }
            writer.write(headStr);
            writer.newLine();

            //写入内容
            final String startTimeFormat = searchVo.getStartTimeFormat();

            Loop1:
            for (Map<String, Object> map : searchVo.getData()) {

                StringBuilder builder = new StringBuilder(startTimeFormat + "," + nodeName);

                Object count = map.get(EsService.COUNT_FIELD_NAME);
                count = count == null ? 0 : count;

                Object _latency_msec = map.get(EsService.LATENCY_MSEC_FIELD_NAME);
                _latency_msec = _latency_msec == null ? 0 : _latency_msec;

                Object response = map.get(EsService.RESPONSE_FIELD_NAME);
                response = response == null ? 0 : response;


                builder.append(",").
                        append(count).
                        append(",").
                        append(_latency_msec).
                        append(",").
                        append(response);

                for (String string : fieldStrList) {
                    Object o = map.get(string);
                    final boolean b = o == null;
                    if (DELETE_HAS_NULL_VALUE_LINE && b) {
                        continue Loop1;
                    }
                    o = b ? "" : o;
                    builder.append(",").append(o);
                }

                writer.write(builder.toString());
                writer.newLine();
            }
            LOGGER.info("导出 cvs 文件, 路径= {} , 节点= {} ,文件目录= {}", topoName, nodeName, csvFile.getPath());

        } catch (Exception e) {
            LOGGER.error("导出 cvs 文件过程错误 ,路径= {} , 节点= {} ,error={}", topoName, nodeName, e.getMessage());
        } finally {
            try {
                if (writer != null) {
                    writer.close();
                }
            } catch (IOException e) {
                LOGGER.error("导出 cvs 文件关闭 IO 错误 ,路径= {} , 节点= {} ,error={}", topoName, nodeName, e.getMessage());
            }
        }
    }

    /**
     * 启动任务
     */
    public void invoke() {

        timer.scheduleAtFixedRate(new TimerTaskHandle(this), 0L, 60 * 1000L);
    }

    public SystemProperties getSystemProperties() {
        return systemProperties;
    }

    public EsService getEsService() {
        return esService;
    }

    public void closeTimerTask() {
        timer.cancel();
    }


    /**
     * 实现定时任务
     */
    class TimerTaskHandle extends TimerTask {

        private CoreService coreService;

        public TimerTaskHandle(CoreService coreService) {
            this.coreService = coreService;
        }

        /**
         * The action to be performed by this timer task.
         */
        @Override
        public void run() {

            //查询时间计算
            final int calculationSecondsAhead = Integer.parseInt(coreService.getSystemProperties().getCalculationSecondsAhead());
            long currentTimeMillis = System.currentTimeMillis() / 1000L * 1000L;
            currentTimeMillis -= currentTimeMillis % MILLISECOND_IN_MINUTE;
            //开始时间延迟
            long startTimeMsc = (currentTimeMillis - 1000L * calculationSecondsAhead);
            long endTimeMsc = startTimeMsc + MILLISECOND_IN_MINUTE;

            final List<SearchVo> search = coreService.esService.search(startTimeMsc, endTimeMsc);

            for (SearchVo vo : search) {
                createCSVFile(vo, coreService.getSystemProperties().getExportCVSFilePath());
            }

        }
    }
}
