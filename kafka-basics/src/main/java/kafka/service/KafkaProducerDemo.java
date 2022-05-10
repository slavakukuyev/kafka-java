package kafka.service;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class.getSimpleName());
    private static final String KafkaTopic = "wikimedia.recentchange3";

    public static void main(String[] args) throws InterruptedException {
        // create producer options
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //if we have 3 brokers then configuration of insync replica has to be updated to 2
        //min.insync.replicas=2  // https://kafka.apache.org/documentation/#min.insync.replicas
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        //create topic
        //kafka-topics --bootstrap-server "127.0.0.1:9092" --create --topic wikimedia.recentchange3 --partitions 3 --replication-factor 3 --config "min.insync.replicas=2"
        /*
        Request:
        kafka-topics --bootstrap-server "127.0.0.1:9092" --describe --topic wikimedia.recentchange3
        Result:
        Topic: wikimedia.recentchange3	TopicId: GqANqU2qSPibkgTVVqdWwQ	PartitionCount: 3	ReplicationFactor: 3	Configs: min.insync.replicas=2
	    Topic: wikimedia.recentchange3	Partition: 0	Leader: 3	Replicas: 3,2,1	Isr: 3,2,1
	    Topic: wikimedia.recentchange3	Partition: 1	Leader: 1	Replicas: 1,3,2	Isr: 1,3,2
	    Topic: wikimedia.recentchange3	Partition: 2	Leader: 2	Replicas: 2,1,3	Isr: 2,1,3


	    I restarted one broker, producer worked as expected,
	    then I restarted 2 brokers and during the restarts got similar errors:
	    Got error produce response with correlation id 4224 on topic-partition wikimedia.recentchange3-0, retrying (2147483428 attempts left). Error: NOT_ENOUGH_REPLICAS
         */


        /*
            Idempotent Producer
         */
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));// 2147483647
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");//kafka version >= 1.0
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");//do not save the same message twice
        properties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000"); //delivery timeout of message in ms (2 minutes)


        /*
        Compression type	Compression ratio	CPU usage	Compression speed	Network bandwidth usage
                    Gzip	Highest	            Highest	    Slowest	            Lowest
                    Snappy	Medium	            Moderate	Moderate            Medium
                    Lz4	    Low	                Lowest	    Fastest	            Highest
                    Zstd	Medium	            Moderate	Moderate            Medium

         You will need to upgrade Kafka if it's older than version 2.1.0 to use Zstd compression, otherwise use snappy
         */

        //high throughput producer config
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));

        /*
        Exception will be thrown after third step:
         1. Producer will fill memory buffer
         2. .send() will be blocked // means that broker doesn't accept any data frm the producer
         3. 60ms elapsed after block
         */
        properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
        properties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "60000");


        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //listen for messages:
        //kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic wikimedia.recentchange3


        EventHandler handler = new WikimediaHandler(producer, KafkaTopic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder = new EventSource.Builder(handler, URI.create(url));
        EventSource eventsource = builder.build();

        eventsource.start();

        //run code 10 minutes then exit
        TimeUnit.MINUTES.sleep(10);

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
