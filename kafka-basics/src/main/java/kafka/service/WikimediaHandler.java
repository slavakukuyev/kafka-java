package kafka.service;


import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaHandler implements EventHandler {
    KafkaProducer<String, String> kafkaProducer;
    String topic;

    private final Logger log = LoggerFactory.getLogger(WikimediaHandler.class.getSimpleName());


    public WikimediaHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }


    @Override
    public void onOpen() {

    }

    @Override
    public void onClosed() {
        this.kafkaProducer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) {

        String message = messageEvent.getData();
        log.info(message);
        this.kafkaProducer.send(new ProducerRecord<>(topic, message));

    }

    @Override
    public void onComment(String comment) {

    }

    @Override
    public void onError(Throwable t) {
        log.error("Error in stream of messages", t);
    }
}
