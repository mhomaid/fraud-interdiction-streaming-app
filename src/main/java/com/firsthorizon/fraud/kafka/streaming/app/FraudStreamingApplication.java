package com.firsthorizon.fraud.kafka.streaming.app;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import javax.annotation.PostConstruct;
import javax.xml.bind.JAXBException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;


@Component
public class FraudStreamingApplication {
    @Autowired
    private Environment env;
    private static final Logger Log = LoggerFactory.getLogger(FraudStreamingApplication.class);
    private static final String APPLICATION_ID = "application.id";
    private static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    private static final String MONITORING_PRODUCER_INTERCEPTOR = "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor";
    private static final String MONITORING_CONSUMER_INTERCEPTOR = "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor";
    private static final String INPUT_FRAUD_RAW_TOPIC = "input.fraud.raw.topic.name";
    private static final String OUTPUT_FRAUD_JSON_TOPIC = "output.fraud.json.topic.name";
    private static final String ENVIRONMENT = "environment";
    private static final String CONFIGURATION = "configuration/";
    private static final String PROPERTIES = ".properties";

    public Properties buildStreamsProperties(Properties envProps) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.get(APPLICATION_ID));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.get(BOOTSTRAP_SERVERS));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.PRODUCER_PREFIX + ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, MONITORING_PRODUCER_INTERCEPTOR);
        props.put(StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, MONITORING_CONSUMER_INTERCEPTOR);
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class.getName());
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return props;
    }

    public Topology buildTopology(Properties envProps) {
        final StreamsBuilder builder = new StreamsBuilder();
        final String inputFraudRawTopic = envProps.getProperty(INPUT_FRAUD_RAW_TOPIC);
        final String outputTopicName = envProps.getProperty(OUTPUT_FRAUD_JSON_TOPIC);
        KStream<String, String> rawDataStream = builder.stream(inputFraudRawTopic);
        // Add logic to process the rawDataStreamgit c
        rawDataStream.to(outputTopicName, Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    public Properties loadEnvProperties(String fileName) throws IOException, URISyntaxException {
        Resource resource = new ClassPathResource(fileName);
        InputStream inputStream = resource.getInputStream();
        Properties envProps = new Properties();
        envProps.load(inputStream);
        inputStream.close();
        return envProps;
    }

    @PostConstruct
    public void init() throws Exception {
        String environment = env.getProperty(ENVIRONMENT);
        String propsFile = CONFIGURATION + environment + PROPERTIES;
        FraudStreamingApplication fraudStreamingApplication = new FraudStreamingApplication();
        Properties envProps = fraudStreamingApplication.loadEnvProperties(propsFile);
        Properties streamProps = fraudStreamingApplication.buildStreamsProperties(envProps);
        Topology topology = fraudStreamingApplication.buildTopology(envProps);

        final KafkaStreams streams = new KafkaStreams(topology, streamProps);
        final CountDownLatch latch = new CountDownLatch(1);

        // Attach shutdown handler to catch Control-C.
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });
        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
