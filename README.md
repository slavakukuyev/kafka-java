# kafka-java
Kafka with Java From Basic to Advanced Level


```
My first project written in Java
I just want to practice my key tapping skills with Java and learn Kafka.
```


###Let's run:

IntelliJ IDEA:
met such error:

```Deprecated Gradle features were used in this build, making it incompatible with Gradle 8.0.```

Tips:
* preferences -> Gradle -> Build and run using IntelliJ IDEA
* to run multiple instances of single file: Modify run configurations -> More options -> allow multiple instances
* QA of three consumers create topic with three partitions: ```kafka-topics --bootstrap-server "127.0.0.1:9092" --create --topic KafkaDemoTopic1 --partitions 3 --replication-factor 1
  Created topic KafkaDemoTopic1.```
