#!/bin/sh
##使用软链接启动

java -Xms300M -Xmx300M -Djava.rmi.server.hostname=192.168.1.233 -Dcom.sun.management.jmxremote.port=9999  -Dcom.sun.management.jmxremote.ssl=false  -Dcom.sun.management.jmxremote.authenticate=false -cp ./../ezsonar-ExportMessage-CMB-jar-with-dependencies.jar com.fusionskye.ezsonar.message.Main &
