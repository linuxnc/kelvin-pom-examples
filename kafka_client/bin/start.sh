###启动
#!/bin/sh
moduleName="kafka_client"
pidPath="/var/run/$moduleName-tpid"
rm -f $pidPath
nohup java -jar ./$moduleName.jar -server -Xms1024m -Xmx2048m -Xss256k > ./run.log 2>&1 &
echo $! > $pidPath