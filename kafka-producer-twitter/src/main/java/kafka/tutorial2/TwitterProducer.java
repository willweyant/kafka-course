package kafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    private static final String CONSUMER_KEY = "WpdnVxfiVy2FNW60CB5ZAq8Jj";
    private static final String CONSUMER_SECRET = "qCTQuSaKNnAPXLP7LknqUtXby2nuX4elv8dwwQr70rqXhcuu1V";
    private static final String ACCESS_TOKEN = "1004905662583922689-qtpUr7sTw9a8brvQI8wQUvgurzghbz";
    private static final String ACCESS_TOKEN_SECRET = "9rnoJkVJCumOBkQ1JBRz1okhIoetBw2ar8DZkgrldJ01l";
    private final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
    private final List<String> searchTerms = Lists.newArrayList("trump", "soccer");

    public TwitterProducer() {
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        logger.info("Setting up Twitter client");
        // create a twitter client
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        final BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
        final Client twitterClient = createTwitterClient(msgQueue);
        twitterClient.connect();

        // create a kafka producer
        final KafkaProducer<String, String> producer = createKafkaProducer();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("stopping application...");
            logger.info("shutting down client from twitter...");
            twitterClient.stop();
            logger.info("closing producer...");
            producer.close();
            logger.info("done!");
        }));

        // loop to send tweets to kafka
        // on a different thread, or multiple different threads....
        while (!twitterClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.error("Twitter client polling interrupted!", e);
                twitterClient.stop();
            }

            if (msg != null) {
                logger.info("msg = " + msg);
                // NOTE: Create topic in command line:
                // kafka-topics.sh --bootstrap-server localhost:9092 --create --topic twitter_tweets --partitions 6 --replication-factor 1
                // Note: Also create a consumer
                // kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic twitter_tweets
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.error("Exception sending to producer", e);
                        }
                    }
                });
            }
        }

        logger.info("End of application!");
    }

    private Client createTwitterClient(final BlockingQueue<String> msgQueue) {
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        final Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        final StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        hosebirdEndpoint.trackTerms(searchTerms);

        // These secrets should be read from a config file
        final Authentication hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET);

        final ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        final Client hosebirdClient = builder.build();

        return hosebirdClient;
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        final String bootstrapServer = "127.0.0.1:9092";

        // Create Producer properties
        // Find options here: https://kafka.apache.org/documentation/#producerconfigs
        final Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create safe Producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // create high throughput Producer (at the expense of a bit of latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32KB

        // Create the producer
        final KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        return producer;
    }
}
