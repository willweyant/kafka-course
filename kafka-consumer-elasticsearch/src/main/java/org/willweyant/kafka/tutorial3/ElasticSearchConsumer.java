package org.willweyant.kafka.tutorial3;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {
    public static void main(String[] args) throws IOException {
        final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);
        final RestHighLevelClient client = createClient();

        // create the consumer
        final KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");

        // Poll for new data
        while (true) {
            final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            logger.info("Received {} records.", records.count());
            for (ConsumerRecord<String, String> record : records) {
                // insert data into elasticsearch

                // 2 strategies for creating ids:
                // 1) Kafka generic id
//                final String genericId = record.topic() + "_" + record.partition() + "_" + record.offset();

                // 2) Twitter feed specific id
                final String tweetId = extractIdFromTweet(record.value());

                // Note: The index must exist in bonsai
                // PUT /twitter in Bonsai Console
                final IndexRequest indexRequest = new IndexRequest("twitter")
                        .source(record.value(), XContentType.JSON)
                        .id(tweetId); // id makes consumer idempotent

                final IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                final String id = indexResponse.getId();

                logger.info("id = {}", id);

                try {
                    Thread.sleep(10); // introduce small delay
                } catch (InterruptedException e) {
                    logger.error("Error in thread sleep.", e);
                }
            }
            logger.info("Committing offsets...");
            consumer.commitSync();
            logger.info("Offsets have been committed");

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.error("Error in thread sleep.", e);
            }
        }

        // close the client
//        client.close();
    }

    public static RestHighLevelClient createClient() {
        final String hostname = "kafka-course-5064846199.us-east-1.bonsaisearch.net";
        final String username = "e0re025atc";
        final String password = "xenu0uu9wz";

        // Don't do this for local ElasticSearch
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        final RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        final RestHighLevelClient client = new RestHighLevelClient(builder);

        return client;
    }

    public static KafkaConsumer<String, String> createConsumer(final String topic) {
        final String bootstrapServer = "localhost:9092";
        final String groupId = "kafka-demo-elasticsearch"; // When changed, resets the application and all messages are triggered

        final Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // earliest, latest, or none
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable auto-commit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");

        // Create Consumer
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }

    private static String extractIdFromTweet(final String tweetJson) {
        // gson library to extract Id from JSON
        return JsonParser.parseString(tweetJson)
                .getAsJsonObject()
                .get("id_str") // id_str is an attribute of the Twitter tweet JSON
                .getAsString();
    }
}
