package com.fusionskye.ezsonar.message.service;


import com.fusionskye.ezsonar.message.Main;
import com.fusionskye.ezsonar.message.dao.MongoDBClient;
import com.fusionskye.ezsonar.message.model.Connection;
import com.fusionskye.ezsonar.message.model.Node;
import com.fusionskye.ezsonar.message.model.Topo;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.bson.types.BasicBSONList;
import org.bson.types.ObjectId;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author wei.Li by 15/12/7
 */
public final class TopoService {

    /**
     * 所有别名
     * topoId   , 别名
     * nodeName , 别名
     */
    private static final Map<String, String> ALIAS = Maps.newHashMap();

    private static final Logger LOGGER = LoggerFactory.getLogger(TopoService.class);

    //查询 Topo 对象字段
    private static final BasicDBObject BASIC_DB_KEYS = new BasicDBObject("name", 1).append("connections", 1).append("nodes", 1);
    //待统计的业务路径 id, 节点name
    private static final Map<String, List<String>> TOPO_ID_NODE_NAMES = Maps.newConcurrentMap();
    //待统计的业务路径封装对象
    private static final List<Topo> TOPO_STATISTICS = Lists.newArrayList();

    static {
        try {
            analyzerTopoNodeNameFile();
        } catch (Exception e) {
            Main.exitSystem("解析路径,节点名称配置文件过程错误 error=" + e);
        }
    }

    /**
     * 解析需要统计的路径-节点配置文件
     *
     * @throws Exception 解析过程错误
     */
    private static void analyzerTopoNodeNameFile() throws Exception {

        FileInputStream fileInputStream = null;
        try {

            fileInputStream = new FileInputStream(Main.TOPO_NODE_XML_FILE_PATH);
            SAXReader reader = new SAXReader();
            Document document = reader.read(fileInputStream);
            final Element rootElement = document.getRootElement();
            if (rootElement == null) {
                return;
            }

            Map<String, List<String>> map = Maps.newHashMap();

            int i0 = 0;
            final List elements = rootElement.elements();
            for (Object o : elements) {
                final Element o1 = (Element) o;
                final String topoId = o1.attribute("id").getValue();
                final String alias = o1.attribute("alias").getValue();
                checkArgument(!topoId.isEmpty(), "路径 id 不能为空");
                checkArgument(!alias.isEmpty(), "路径 " + topoId + " 别名不能为空");
                ALIAS.put(topoId, alias);
                i0++;
                final Element statisticsNodes = o1.element("statisticsNodes");
                final List nodeElements = statisticsNodes.elements();
                if (!nodeElements.isEmpty()) {
                    List<String> nodeNames = Lists.newArrayList();
                    for (Object nodeElement : nodeElements) {
                        final Element node = ((Element) nodeElement);
                        final String nodeName = node.attribute("name").getValue();
                        final String aliasNodeName = node.attribute("alias").getValue();
                        checkArgument(!nodeName.isEmpty(), "节点 name 不能为空");
                        checkArgument(!aliasNodeName.isEmpty(), "节点 " + nodeName + " 别名不能为空");
                        ALIAS.put(nodeName, aliasNodeName);
                        i0++;
                        nodeNames.add(nodeName);
                    }
                    map.put(topoId, nodeNames);
                }
            }

            checkArgument(Sets.newHashSet(ALIAS.values()).size() == i0, "解析路径,节点名称配置文件过程错误,别名配置有重复");

            TOPO_ID_NODE_NAMES.clear();
            TOPO_ID_NODE_NAMES.putAll(map);
            LOGGER.info("配置文件解析结果为 待统计路径 , 节点名称  =  {}", map);
        } finally {
            if (fileInputStream != null) {
                fileInputStream.close();
            }
        }
    }


    /**
     * 将配置文件路径,节点 查询数据库后映射为 {@link Topo}
     */
    public static void analyzerTopoForDb() {

        LOGGER.debug("invoke analyzerTopoForDb...");

        //将路径 id 转换为 {$in:[...]} 格式
        final Set<String> topoIds = TOPO_ID_NODE_NAMES.keySet();
        final int topoIdSize = topoIds.size();
        final String[] topoIdsArray = topoIds.toArray(new String[topoIdSize]);

        ObjectId[] objectIds = new ObjectId[topoIdSize];
        for (int i = 0; i < topoIdSize; i++) {
            objectIds[i] = new ObjectId(topoIdsArray[i]);
        }

        final DBCursor dbObjects = MongoDBClient.getDB().getCollection("topos").find(
                new BasicDBObject("_id", new BasicDBObject("$in", objectIds)),
                BASIC_DB_KEYS
        );

        //解析查询结果,映射为 topo 对象
        List<Topo> topoList = Lists.newArrayList();

        while (dbObjects.hasNext()) {
            final DBObject next = dbObjects.next();

            final Object id = next.get("_id");
            final Object name = next.get("name");

            List<Connection> connectionList = Lists.newArrayList();
            final BasicBSONList connections = ((BasicBSONList) next.get("connections"));
            for (Object connection : connections) {
                final BasicDBObject basicDBObject = (BasicDBObject) connection;
                try {
                    connectionList.add(
                            new Connection(basicDBObject.get("source").toString(),
                                    basicDBObject.get("target").toString(),
                                    basicDBObject.get("streamid").toString())
                    );
                } catch (Exception e) {
                    LOGGER.warn("配置文件匹配数据库记录解析 查询数据库连接线过程中 路径 id= {} name= {} 解析出错 error= {} , 默认不进行统计,请核对", id, name, e);
                }
            }

            List<Node> nodeList = Lists.newArrayList();
            final BasicBSONList nodes = ((BasicBSONList) next.get("nodes"));
            for (Object o : nodes) {
                final BasicDBObject basicDBObject = (BasicDBObject) o;
                try {
                    nodeList.add(
                            new Node(basicDBObject.get("node_id").toString(),
                                    basicDBObject.get("name").toString(),
                                    basicDBObject.get("streamDirection").toString())
                    );
                } catch (Exception e) {
                    LOGGER.warn("配置文件匹配数据库记录解析 查询数据库节点过程中 路径 id= {} name= {} 解析出错 error= {} , 默认不进行统计,请核对", id, name, e);
                }
            }

            topoList.add(new Topo(id.toString(), name.toString(), nodeList, connectionList));
        }


        //比对配置文件的 nodeName 与数据库中时候匹配,同时计算每个节点统计时所需的流id
        for (Iterator<Topo> iterator = topoList.iterator(); iterator.hasNext(); ) {
            Topo topo = iterator.next();
            final String topoId = topo.getId();
            final List<String> nodeNames = TOPO_ID_NODE_NAMES.get(topoId);
            if (nodeNames == null) {
                iterator.remove();
                continue;
            }

            final List<String> noMatchNodeNames = topo.fetchNodeNameStreamIdsByStreamDirection(nodeNames);
            final List<Node> nodeList = topo.getNodeList();
            if (nodeList.isEmpty()) {
                iterator.remove();
            }
            if (noMatchNodeNames.isEmpty()) {
                LOGGER.info("配置文件匹配数据库记录解析结果为 路径 id= {} name= {} . 待统计节点名称 数量= {} ,名称= {}",
                        topoId, topo.getName(), nodeList.size(), nodeList);
            } else {
                LOGGER.error("配置文件匹配数据库记录解析结果为 路径 id= {} name= {} , 不匹配节点名称  数量= {} ,名称= {} . 待统计节点名称 数量= {} ,名称= {}",
                        topoId, topo.getName(), noMatchNodeNames.size(), noMatchNodeNames, nodeList.size(), nodeList);
            }
        }

        synchronized (TOPO_STATISTICS) {
            TOPO_STATISTICS.clear();
            TOPO_STATISTICS.addAll(topoList);
        }
    }


    public static List<Topo> getTopoStatistics() {
        return TOPO_STATISTICS;
    }

    public static String getAliasName(String id) {
        return ALIAS.get(id);
    }
}
