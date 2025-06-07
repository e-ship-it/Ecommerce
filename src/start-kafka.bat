@echo off

REM =======Start Zookeeper=============
Start cmd /k "cd /d C:\kafka && bin\Windows\zookeeper-server-start.bat  config\zookeeper.properties"

REM ======= Wait ======================
:ZookeeperWAIT
netstat -ano | findstr 2181
echo %ERRORLEVEL%
if %ERRORLEVEL% NEQ 0 (
    echo Zookeeper is not ready. Retrying...
    timeout /t 5
    goto ZookeeperWAIT
)

timeout /t 5
REM ========Start Kafka ===============
start cmd /k "cd /d C:\kafka && bin\Windows\kafka-server-start config\server.properties"

REM ======= Wait ======================
:KafkaWAIT
netstat -ano | findstr 9092
echo %ERRORLEVEL%
if %ERRORLEVEL% NEQ 0 (
    echo Kafka is not ready. Retrying...
    timeout /t 5
    goto KafkaWAIT
)