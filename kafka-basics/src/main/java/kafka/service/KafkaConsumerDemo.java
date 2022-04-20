package kafka.service;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;


public class KafkaConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class.getSimpleName());
    private static final String KafkaDemoTopic = "KafkaDemoTopic3";

    public static void main(String[] args) {
        // create consumer options
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");

        //as in producer with serialize kafka keys/messages, consumers have to deserialize keys/messages into string
        //NOTE!!! use StringDeserializer  //Copied from producer and missed the class name, so spent for this about 10 minutes
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //GROUP_ID
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "KAFKA-CONSUMER-GROUP-ID");
        //  properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "TRUE");

        //partition.assignment.strategy
        //by default its (RangeAssignor, CooperativeStickyAssignor) //you can set list of strategies
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        //enable static assignment for partitions group.instance.id
        //each instance ID has to be with different name. Usually it's some env var with identical instance name
        //if after session.timeout.ms = 45000 (ms default value) consumer connects it polls data from its partition without reassignment of consumers
        properties.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "instance1");
        //properties.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "instance2");
        //properties.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "instance3");
        properties.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");//after 10s consumers will reassign


        //none - if there are no messages, don't start consumer
        //earliest - read messages of the topic from the beginning
        //latest - read messages with timestamp after consumer started
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //subscribe to single topic
        consumer.subscribe(Collections.singleton(KafkaDemoTopic));
        //OR
        //subscribe to list of topics
//        consumer.subscribe(Arrays.asList(KafkaDemoTopic));


        //Important notice to work with IntelliJ: when you are in debug/run mode, you have an "Exit" Button in your "Run"/"Debug" panel usually on the right side at the bottom
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected shutdown, lets wakeup the consumer");
            consumer.wakeup();

            try {
                mainThread.join();
            } catch (
                    InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {
            while (true) {//!!!don't use such code in prod
                log.info("Polling...");

                //poll messages if exist or wait for 1500 ms and then poll
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1500));


                for (ConsumerRecord<String, String> record : records) {
                    log.info("\nThe record sent successfully to kafka: \n" +
                            "Value: " + record.value() + "\n" +
                            "Key: " + record.key() + "\n" +
                            "Partition: " + record.partition() + "\n" +
                            "Offset: " + record.offset() + "\n");
                }
            }
        } catch (WakeupException e) {
            log.info("Wakeup exception");
        } catch (Exception e) {
            log.error("Unexpected exception", e);
        } finally {
            log.info("closing consumer...");
            consumer.close();
        }
    }
}

//run consumer and producer in parallel to watch on received messages

/*
 INFO org.apache.kafka.clients.consumer.ConsumerConfig - ConsumerConfig values:
	allow.auto.create.topics = true
	auto.commit.interval.ms = 5000
	auto.offset.reset = earliest
	bootstrap.servers = [127.0.0.1:9092]
	check.crcs = true
	client.dns.lookup = use_all_dns_ips
	client.id = consumer-KAFKA-CONSUMER-GROUP-ID-instance1
	client.rack =
	connections.max.idle.ms = 540000
	default.api.timeout.ms = 60000
	enable.auto.commit = true
	exclude.internal.topics = true
	fetch.max.bytes = 52428800
	fetch.max.wait.ms = 500
	fetch.min.bytes = 1
	group.id = KAFKA-CONSUMER-GROUP-ID
	group.instance.id = instance1
	heartbeat.interval.ms = 3000
	interceptor.classes = []
	internal.leave.group.on.close = true
	internal.throw.on.fetch.stable.offset.unsupported = false
	isolation.level = read_uncommitted
	key.deserializer = class org.apache.kafka.common.serialization.StringDeserializer
	max.partition.fetch.bytes = 1048576
	max.poll.interval.ms = 300000
	max.poll.records = 500
	metadata.max.age.ms = 300000
	metric.reporters = []
	metrics.num.samples = 2
	metrics.recording.level = INFO
	metrics.sample.window.ms = 30000
	partition.assignment.strategy = [org.apache.kafka.clients.consumer.CooperativeStickyAssignor]
	receive.buffer.bytes = 65536
	reconnect.backoff.max.ms = 1000
	reconnect.backoff.ms = 50
	request.timeout.ms = 30000
	retry.backoff.ms = 100
	sasl.client.callback.handler.class = null
	sasl.jaas.config = null
	sasl.kerberos.kinit.cmd = /usr/bin/kinit
	sasl.kerberos.min.time.before.relogin = 60000
	sasl.kerberos.service.name = null
	sasl.kerberos.ticket.renew.jitter = 0.05
	sasl.kerberos.ticket.renew.window.factor = 0.8
	sasl.login.callback.handler.class = null
	sasl.login.class = null
	sasl.login.connect.timeout.ms = null
	sasl.login.read.timeout.ms = null
	sasl.login.refresh.buffer.seconds = 300
	sasl.login.refresh.min.period.seconds = 60
	sasl.login.refresh.window.factor = 0.8
	sasl.login.refresh.window.jitter = 0.05
	sasl.login.retry.backoff.max.ms = 10000
	sasl.login.retry.backoff.ms = 100
	sasl.mechanism = GSSAPI
	sasl.oauthbearer.clock.skew.seconds = 30
	sasl.oauthbearer.expected.audience = null
	sasl.oauthbearer.expected.issuer = null
	sasl.oauthbearer.jwks.endpoint.refresh.ms = 3600000
	sasl.oauthbearer.jwks.endpoint.retry.backoff.max.ms = 10000
	sasl.oauthbearer.jwks.endpoint.retry.backoff.ms = 100
	sasl.oauthbearer.jwks.endpoint.url = null
	sasl.oauthbearer.scope.claim.name = scope
	sasl.oauthbearer.sub.claim.name = sub
	sasl.oauthbearer.token.endpoint.url = null
	security.protocol = PLAINTEXT
	security.providers = null
	send.buffer.bytes = 131072
	session.timeout.ms = 10000
	socket.connection.setup.timeout.max.ms = 30000
	socket.connection.setup.timeout.ms = 10000
	ssl.cipher.suites = null
	ssl.enabled.protocols = [TLSv1.2, TLSv1.3]
	ssl.endpoint.identification.algorithm = https
	ssl.engine.factory.class = null
	ssl.key.password = null
	ssl.keymanager.algorithm = SunX509
	ssl.keystore.certificate.chain = null
	ssl.keystore.key = null
	ssl.keystore.location = null
	ssl.keystore.password = null
	ssl.keystore.type = JKS
	ssl.protocol = TLSv1.3
	ssl.provider = null
	ssl.secure.random.implementation = null
	ssl.trustmanager.algorithm = PKIX
	ssl.truststore.certificates = null
	ssl.truststore.location = null
	ssl.truststore.password = null
	ssl.truststore.type = JKS
	value.deserializer = class org.apache.kafka.common.serialization.StringDeserializer
 */
