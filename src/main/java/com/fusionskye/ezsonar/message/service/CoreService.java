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
 * 执行定时任务,导出 csv 文件
 *
 * @author wei.Li by 15/12/8
 */
public class CoreService {

    private static final Logger LOGGER = LoggerFactory.getLogger(CoreService.class);

    /**
     * 1分钟毫秒数
     */
    private static final long MILLISECOND_IN_MINUTE = 60 * 1000L;

    private final Timer timer = new Timer();

    private SystemProperties systemProperties;
    private EsService esService;

    public CoreService(SystemProperties systemProperties) {
        this.systemProperties = systemProperties;
        this.esService = new EsService(this);
    }


    /**
     * 导出 csv 文件
     * EZSonar 导出的数据存放路径如下，字母命名， 文件名使用日期&时间格式， 例如 “201512031200”
     * 文件为一分钟一个文件，目录结构：
     * 一级目录:业务路径，如***系统
     * 二级目录:逻辑节点，如***应用服务器
     * 二级目录:存放一分钟一个的文件
     *
     * @param searchVo       searchVo
     * @param outPutRootPath 导出文件主目录
     */
    private void createCSVFile(SearchVo searchVo, String outPutRootPath) {

        BufferedWriter writer = null;

        final String encoding = this.getSystemProperties().getExportCVSFileEncoding();
        final String topoName = searchVo.getTopoName();
        String topoId = searchVo.getTopoId();
        final String aliasTopoName = TopoService.getAliasName(topoId);

        final String nodeName = searchVo.getNode().getName();

        final String aliasNodeName = TopoService.getAliasName(topoId + nodeName);
        final String pathname = outPutRootPath + File.separator + aliasTopoName + File.separator + aliasNodeName;

        String fileNamePrefix = pathname + File.separator + searchVo.getFileNameStartTimeFormat();
        File csvTempFile = new File(fileNamePrefix + ".temp");

        try {

            Files.createParentDirs(csvTempFile);
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(csvTempFile), encoding), 1024);

            // 写入文件头部

            final Set<Map.Entry<String, String>> entries = EsService.getStatisticsGroupFieldMap().entrySet();

            StringBuilder headStr = new StringBuilder("时间,节点名称,交易数量,响应数量,响应时间");
            List<String> headStrList = Lists.newArrayList();
            List<String> fieldStrList = Lists.newArrayList();

            for (Map.Entry<String, String> entry : entries) {
                final String key = entry.getKey();
                fieldStrList.add(key);

                final String value = entry.getValue();
                headStrList.add(value);
            }

            //是否显示表头
            if (this.getSystemProperties().getIsShowTableHead().equalsIgnoreCase(Boolean.TRUE.toString())) {
                for (String value : headStrList) {
                    headStr.append(",").append(value);
                }
                headStr.append("\r\n");
                writer.write(headStr.toString());
            }

            //写入内容
            final String startTimeFormat = searchVo.getStartTimeFormat();

            StringBuilder builder;
            Loop1:
            for (Map<String, Object> map : searchVo.getData()) {

                builder = new StringBuilder(startTimeFormat + "," + aliasNodeName);

                Object count = map.get(EsService.COUNT_FIELD_NAME);
                count = count == null ? 0 : count;
                Object latencyMsec = map.get(EsService.LATENCY_MSEC_FIELD_NAME);
                latencyMsec = latencyMsec == null ? 0 : latencyMsec;
                Object response = map.get(EsService.RESPONSE_FIELD_NAME);
                response = response == null ? 0 : response;

                builder.append(",").append(count).append(",").append(response).append(",").append(latencyMsec);

                //拼接统计字段值
                for (String string : fieldStrList) {
                    Object o = map.get(string);
                    final boolean b = o == null;
                    if (b) {
                        continue Loop1;
                    }
                    builder.append(",").append(o);
                }
                builder.append("\r\n");
                writer.write(builder.toString());
                //writer.newLine();
            }
            File to = new File(fileNamePrefix + ".csv");
            csvTempFile.renameTo(to);
            csvTempFile.delete();
            LOGGER.info("导出 csv 文件, 路径= {} 别名={}, 节点= {} 别名={} , 内容大小= {} , 文件目录= {}",
                    topoName, aliasTopoName, nodeName, aliasNodeName, searchVo.getData().size(), to.getPath());

        } catch (IOException e) {
            LOGGER.error("导出 csv 文件过程错误 ,路径= {} , 节点= {} ,error={}", topoName, nodeName, e.getMessage());
        } finally {
            try {
                if (writer != null) {
                    writer.flush();
                    writer.close();
                }
            } catch (IOException e) {
                LOGGER.error("导出 csv 文件关闭 IO 错误 ,路径= {} , 节点= {} ,error={}", topoName, nodeName, e.getMessage());
            }
        }
    }

    /**
     * 启动任务
     */
    public void invoke() {
        //  运行时间为 每分钟第 55 秒
        int i = Calendar.getInstance().get(Calendar.SECOND);
        int i1 = 55 - i;
        i1 = i1 < 0 ? (60 - i + 55) : i1;

        timer.scheduleAtFixedRate(new ExportMessageTimerTask(this), i1 * 1000L, MILLISECOND_IN_MINUTE);
        timer.scheduleAtFixedRate(new RefreshTopoTimerTaskHandle(), 30 * MILLISECOND_IN_MINUTE, 30 * MILLISECOND_IN_MINUTE);
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
     * 实现定时任务 查询 ES , 导出 ES 查询结果数据
     */
    private class ExportMessageTimerTask extends TimerTask {

        private CoreService coreService;

        ExportMessageTimerTask(CoreService coreService) {
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

            final String exportCVSFilePath = coreService.getSystemProperties().getExportCVSFilePath();

            final List<SearchVo> search = coreService.esService.search(startTimeMsc, endTimeMsc);
            for (SearchVo vo : search) {
                createCSVFile(vo, exportCVSFilePath);
            }

        }
    }

    /**
     * 实现定时任务 刷新 topo 数据库内容
     */
    private class RefreshTopoTimerTaskHandle extends TimerTask {

        /**
         * The action to be performed by this timer task.
         */
        @Override
        public void run() {

            try {
                TopoService.analyzerTopoForDb();
            } catch (Exception e) {
                LOGGER.error("TopoService#analyzerTopoForDb , error={}", e);
            }
        }
    }
}
