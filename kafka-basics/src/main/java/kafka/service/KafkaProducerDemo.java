package kafka.service;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class.getSimpleName());
    private static final String KafkaDemoTopic = "KafkaDemoTopic3";

    public static void main(String[] args) {
        // create producer options
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        //create topic:
        //kafka-topics --bootstrap-server "127.0.0.1:9092" --create --topic KafkaDemoTopic3 --partitions 3 --replication-factor 1
        //listen for messages:
        //kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic KafkaDemoTopic


        //send data async

        for (int i = 0; i < 10; i++) {

            String recordValue = "Produce message with cb " + i;
            //the same key goes to the same partition
            String recordKey = "id_" + i;

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(KafkaDemoTopic,recordKey, recordValue);

            producer.send(producerRecord, (metadata, err) -> {
                //called when message sent to producer or error thrown

                if (err == null) {
                    log.info("\nThe record sent successfully to kafka: \n" +
                            "Topic: " + metadata.topic() + "\n" +
                            "Key: " + recordKey + "\n" +
                            "Partition: " + metadata.partition() + "\n" +
                            "Offset: " + metadata.offset() + "\n" +
                            "Timestamp: " + metadata.timestamp() + "\n"
                    );
                } else {
                    log.error("Error while producing message: %o" + err);
                }

            });

            //kafka-topics --bootstrap-server "127.0.0.1:9092" --describe --topic KafkaDemoTopic
            //batches of messages inserted to single partition:
            //partitioner.class = class org.apache.kafka.clients.producer.internals.DefaultPartitioner

            //if you want to work in RoundRobin inserting then:
            //use sleep to have some timestamp for letting Kafka send each message to the next partition
            //and when we use key for each message, then each key goes to the same partition
/*            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error("Error on try to sleep " + e);
            }
*/
        }

        //wait up until all data sent to kafka and received by brokers
        //call gives a convenient way to ensure all previously sent messages have actually completed.
        producer.flush(); //sync method

        producer.close();
    }
}

/*
  Kafka properties by default:::

  acks = -1
  	batch.size = 16384
  	bootstrap.servers = [127.0.0.1:9092]
  	buffer.memory = 33554432
  	client.dns.lookup = use_all_dns_ips
  	client.id = producer-1
  	compression.type = none
  	connections.max.idle.ms = 540000
  	delivery.timeout.ms = 120000
  	enable.idempotence = true
  	interceptor.classes = []
  	key.serializer = class org.apache.kafka.common.serialization.StringSerializer
  	linger.ms = 0
  	max.block.ms = 60000
  	max.in.flight.requests.per.connection = 5
  	max.request.size = 1048576
  	metadata.max.age.ms = 300000
  	metadata.max.idle.ms = 300000
  	metric.reporters = []
  	metrics.num.samples = 2
  	metrics.recording.level = INFO
  	metrics.sample.window.ms = 30000
  	partitioner.class = class org.apache.kafka.clients.producer.internals.DefaultPartitioner
  	receive.buffer.bytes = 32768
  	reconnect.backoff.max.ms = 1000
  	reconnect.backoff.ms = 50
  	request.timeout.ms = 30000
  	retries = 2147483647
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
  	transaction.timeout.ms = 60000
  	transactional.id = null
  	value.serializer = class org.apache.kafka.common.serialization.StringSerializer
 */
