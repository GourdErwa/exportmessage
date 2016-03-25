#目前用于招商银行

1.日志输出目录 conf/log4j2.xml   修改<property name="filename"></property> 内容

2.数据库/es 连接等其他配置信息文件 conf/system.properties

3.统计路径->节点配置文件 conf/topoNode.xml

4.启动文件 bin/r.sh


###########  版本说明  ###########

2015年12月09日       1.0


2015年12月10日       1.1

  1.system.properties增加配置项
  #是否显示表头 true,false
  isShowTableHead = false

  2.统计路径->节点配置文件 conf/topoNode.xml增加别名项设置,默认替换 cvs 导出文件夹名称

2015年12月10日       1.2

  1.按需求,csv 中节点名称显示别名

2016年03月25日       1.3

  1.导出数据中增加此是否成功标识符[1成功,0失败或者部分失败,n/a 无法判断]

