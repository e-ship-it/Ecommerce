#WSL (Windows Subsystem for Linux)
cd /d C:\kafka\bin
echo $(pwd) >> C:\Users\Ekta Sharma\Ekta\project\venv\logs\kafka-topic-log.txt 2>&1
#kafka-topics.sh --create --topic my_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
#kafka-topics.sh --list --bootstrap-server localhost:9092
#kafka-console-producer.sh --broker-list localhost:9092 --topic my_topic
topics = $(.\kafka-topics.sh --list --bootstrap localhost:9092)
echo $topics