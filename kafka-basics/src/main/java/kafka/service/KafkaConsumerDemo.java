package kafka.service;

import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


public class KafkaConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerDemo.class.getSimpleName());
    private static final String KafkaDemoTopic = "wikimedia.recentchange3";
    private static final String openSearchIndex = "wikimedia";

    private static String extractRecordId(String jsonString) {
        return JsonParser.parseString(jsonString)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    public static void main(String[] args) throws IOException {
        // create consumer options
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094");

        //as in producer with serialize kafka keys/messages, consumers have to deserialize keys/messages into string
        //NOTE!!! use StringDeserializer  //Copied from producer and missed the class name, so spent for this about 10 minutes
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //GROUP_ID
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "KAFKA-CONSUMER-GROUP-ID-666");

        //partition.assignment.strategy
        //by default its (RangeAssignor, CooperativeStickyAssignor) //you can set list of strategies
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        //enable static assignment for partitions group.instance.id
        //each instance ID has to be with different name. Usually it's some env var with identical instance name
        //if after session.timeout.ms = 45000 (ms default value) consumer connects it polls data from its partition without reassignment of consumers
        properties.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "instance1");
//        properties.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "instance2");
//        properties.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "instance3");
        properties.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");//after 10s consumers will reassign

        /*
        tested. log from console:
            [main] INFO KafkaConsumerDemo - Polling...
            [main] INFO KafkaConsumerDemo - Consumer instance1 received 666 records
            [main] INFO KafkaConsumerDemo - Consumer sent 666 record(s) to OpenSearch
            [main] INFO KafkaConsumerDemo - Offsets have been committed by consumer
            [main] INFO KafkaConsumerDemo - Polling...
        ...
         */
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "666");//number of maximum record on one poll //rename consumer group id to receive all records

        //max.poll.interval.ms - maximum timeout between two .poll() for broker to think consumer is dead/stuck
        properties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "300000");


        //max.partition.fetch.bytes = 1048576  - (1 MB per partition on fetch) if you have 100 partitions you must have 100mb RAM in your app
        properties.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "1048576");

        //fetch.max.bytes = 52428800  - (50MB) max size of messages on single fetch from kafka
        properties.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "52428800");


        // heartbeat.interval.ms = 3000
        //usually heartbeat has to be set by formula: session.timeout.ms / 3
        //consumer sends in intervals messages to broker: "I'm alive"
        properties.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");

        /*
        example of auto commit:
        1.poll messages  (timer started in background)
        2.after 3 seconds poll messages
        3.after 3 seconds poll messages (6s > 5s --> then run in background .commitAsync())
         */
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");//"at-most-once" reading scenario
        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000"); //default


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

        // create an OpenSearch Client
        RestHighLevelClient openSearchClient = OpenSearchClient.createOpenSearchClient();

        //create index in openSearch in case it doesn't exist
        try (openSearchClient; consumer) {
            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest(openSearchIndex), RequestOptions.DEFAULT);

            if (!indexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(openSearchIndex);
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("New index created in open search");
            }

            while (true) {//!!!don't use such code in prod
                log.info("Polling...");

                //poll messages if exist or wait for 1500 ms and then poll
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

                log.info("Consumer {} received {} records", consumer.groupMetadata().groupInstanceId().get(), records.count());

                BulkRequest bulkRequest = new BulkRequest();
                for (ConsumerRecord<String, String> record : records) {

                    //in case we haven't id in record use simple:
                    //String uniqueId = record.topic() + "_" + record.partition() + "_" + record.offset();
                    //otherwise extract id from record:
                    String uniqueId = extractRecordId(record.value());

                    try {
                        IndexRequest indexRequest = new IndexRequest(openSearchIndex)
                                .source(record.value(), XContentType.JSON).id(uniqueId);

                        bulkRequest.add(indexRequest);

                    } catch (Exception e) {
                        log.info("Exception: {}", e);
                    }
                }

                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Consumer sent {} record(s) to OpenSearch", bulkResponse.getItems().length);

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    consumer.commitSync();
                    log.info("Offsets have been committed by consumer");
                }
            }

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
