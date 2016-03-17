package com.fusionskye.ezsonar.message;

import com.fusionskye.ezsonar.message.dao.ESClient;
import com.fusionskye.ezsonar.message.dao.MongoDBClient;
import com.fusionskye.ezsonar.message.model.SystemProperties;
import com.fusionskye.ezsonar.message.service.CoreService;
import com.fusionskye.ezsonar.message.service.TopoService;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author wei.Li by 15/12/7
 */
public final class Main {

    //待统计的 业务路径-节点 配置文件目录,目前采用相对路径
    public static final String
            TOPO_NODE_XML_FILE_PATH = "./conf/topoNode.xml",
            CONF_SYSTEM_PROPERTIES = "./conf/system.properties",
            LOG_4_J_2_PATH = "./conf/log4j2.xml";

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    private static SystemProperties SYSTEM_PROPERTIES_OBJ = null;

    static {
        Properties properties = new Properties();
        FileReader reader = null;
        try {
            reader = new FileReader(CONF_SYSTEM_PROPERTIES);
            properties.load(reader);
            SYSTEM_PROPERTIES_OBJ = SystemProperties.createSystemProperties(properties);

        } catch (Exception e) {
            exitSystem("载入配置文件错误 , error = " + e.getMessage());
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException ignored) {
                }
            }
        }
    }

    public static void main(String[] args) {

        CoreService coreService = new CoreService(SYSTEM_PROPERTIES_OBJ);

        try {
            reLog4j2Configure();

            System.out.println("载入 log4j2 配置文件成功...");
            LOGGER.debug("载入配置文件成功 , 内容 {}", SYSTEM_PROPERTIES_OBJ);


            MongoDBClient.createMongoDBClient(SYSTEM_PROPERTIES_OBJ);

            ESClient.createESClient(SYSTEM_PROPERTIES_OBJ);

            TopoService.analyzerTopoForDb();

            System.out.println("数据库 Es 连接成功...");

            System.out.println("启动任务...");

            coreService.invoke();

            System.out.println("系统运行中...");

            //注册系统钩子函数
            //Runtime.getRuntime().addShutdownHook(new ShutdownHandlerThread(coreService));

        } catch (Exception e) {
            e.printStackTrace();
            exitSystem("初始化任务过程出错");
            //new ShutdownHandlerThread(coreService).run();
        }

    }

    /**
     * 载入 log4j2.xml 配置文件
     */
    private static void reLog4j2Configure() {


        final File file = new File(LOG_4_J_2_PATH);
        checkArgument(file.exists(), "日志配置文件 log4j2.xml 不存在," + file.getPath());

        final Level level = Level.getLevel(SYSTEM_PROPERTIES_OBJ.getLog4j2RunModel().toUpperCase());
        checkNotNull(level, "系统运行日志级别配置 runModel={} 错误");

        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        context.setConfigLocation(file.toURI());
        context.reconfigure();

        for (org.apache.logging.log4j.core.Logger logger : context.getLoggers()) {
            logger.setLevel(level);
        }
    }

    /**
     * @param msg 关闭系统提示信息
     */
    public static void exitSystem(String msg) {
        System.out.println("系统退出,信息:" + msg);
        System.exit(1);
    }

}
