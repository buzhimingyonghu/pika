rm -rf ./log
rm -rf ./db
rm -rf ./dump
rm -rf ./pika_slave/log
rm -rf ./pika_slave/db
rm -rf ./pika_slave/dump
rm -rf ./conf/pika.conf && cp ./conf/pika_copy.conf ./conf/pika.conf
rm -rf ./pika_slave/conf/pika.conf && cp ./conf/pika_copy_2.conf ./pika_slave/conf/pika.conf
rm -rf ./pika_slave/pika && cp ./output/pika ./pika_slave/pika
rm -rf ./pika_slave/dbsync