package io.confluent.kafka.connect.twitter;

import com.google.common.collect.Iterables;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import twitter4j.conf.Configuration;
import twitter4j.conf.PropertyConfiguration;

import java.util.List;
import java.util.Map;
import java.util.Properties;


public class TwitterSourceConnectorConfig extends AbstractConfig {

  public static final String TWITTER_DEBUG_CONF = "twitter.debug";
  private static final String TWITTER_DEBUG_DOC = "Flag to enable debug logging for the twitter api.";
  public static final String TWITTER_DEBUG_DISPLAY = "Twitter debug enabled";
  
  public static final String TWITTER_OAUTH_CONSUMER_KEY_CONF = "twitter.oauth.consumerKey";
  private static final String TWITTER_OAUTH_CONSUMER_KEY_DOC = "OAuth consumer key";
  public static final String TWITTER_OAUTH_CONSUMER_KEY_DISPLAY = "Consumer key";
  
  public static final String TWITTER_OAUTH_SECRET_KEY_CONF = "twitter.oauth.consumerSecret";
  private static final String TWITTER_OAUTH_SECRET_KEY_DOC = "OAuth consumer secret";
  public static final String TWITTER_OAUTH_SECRET_KEY_DISPLAY = "Consumer secret";
  
  public static final String TWITTER_OAUTH_ACCESS_TOKEN_CONF = "twitter.oauth.accessToken";
  private static final String TWITTER_OAUTH_ACCESS_TOKEN_DOC = "OAuth access token";
  public static final String TWITTER_OAUTH_ACCESS_TOKEN_DISPLAY = "Access token";
  
  public static final String TWITTER_OAUTH_ACCESS_TOKEN_SECRET_CONF = "twitter.oauth.accessTokenSecret";
  private static final String TWITTER_OAUTH_ACCESS_TOKEN_SECRET_DOC = "OAuth access token secret";
  public static final String TWITTER_OAUTH_ACCESS_TOKEN_SECRET_DISPLAY = "Access secret";

  public static final String TWITTER_QUEUE_SIZE_CONF = "twitter.queue.size";
  private static final String TWITTER_QUEUE_SIZE_DOC = "Size of buffer queue for tweets to be published to Kafka topic; 0 indicates unlimited";
  public static final String TWITTER_QUEUE_SIZE_DISPLAY = "Twitter queue size";
  
  public static final String FILTER_KEYWORDS_CONF = "filter.keywords";
  private static final String FILTER_KEYWORDS_DOC = "Twitter keywords to filter for.";
  private static final String FILTER_KEYWORDS_DISPLAY = "Keywords";
  private static final String FILTER_KEYWORDS_DEFAULT = "";
  
  public static final String KAFKA_XFER_RATE_CONF = "kafka.xfer.rate";
  public static final String KAFKA_XFER_RATE_DOC = "Maximum rate (tweets per second) to save to Kafka topic; 0 indicates no limit";
  public static final String KAFKA_XFER_RATE_DISPLAY = "Maximum write rate";

  public static final String KAFKA_STATUS_TOPIC_CONF = "kafka.status.topic";
  public static final String KAFKA_STATUS_TOPIC_DOC = "Kafka topic into which status tweets will be published.";
  public static final String KAFKA_STATUS_TOPIC_DISPLAY = "Status topic";
  
  public static final String KAFKA_DELETE_TOPIC_CONF = "kafka.delete.topic";
  public static final String KAFKA_DELETE_TOPIC_DOC = "Kafka topic into which delete events will be published.";
  public static final String KAFKA_DELETE_TOPIC_DISPLAY = "Deletes topic";
  
  public static final String PROCESS_DELETES_CONF = "process.deletes";
  public static final String PROCESS_DELETES_DOC = "Should this connector process deletes.";
  public static final String PROCESS_DELETES_DISPLAY = "Process deletes ? ";

  public static final String ACCOUNT_GROUP = "TwitterAccount";
  public static final String CONNECTOR_GROUP = "Connector";

  public TwitterSourceConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
    super(config, parsedConfig);
  }

  public TwitterSourceConnectorConfig(Map<String, String> parsedConfig) {
    this(CONFIG_DEF, parsedConfig);
  }

  public static ConfigDef conf() {
    return new ConfigDef()
        .define(TWITTER_OAUTH_CONSUMER_KEY_CONF, Type.PASSWORD, Importance.HIGH, TWITTER_OAUTH_CONSUMER_KEY_DOC, ACCOUNT_GROUP, 1, Width.MEDIUM, TWITTER_OAUTH_CONSUMER_KEY_DISPLAY)
        .define(TWITTER_OAUTH_SECRET_KEY_CONF, Type.PASSWORD, Importance.HIGH, TWITTER_OAUTH_SECRET_KEY_DOC, ACCOUNT_GROUP, 2, Width.LONG, TWITTER_OAUTH_SECRET_KEY_DISPLAY)
        .define(TWITTER_OAUTH_ACCESS_TOKEN_CONF, Type.PASSWORD, Importance.HIGH, TWITTER_OAUTH_ACCESS_TOKEN_DOC, ACCOUNT_GROUP, 3, Width.MEDIUM, TWITTER_OAUTH_ACCESS_TOKEN_DISPLAY)
        .define(TWITTER_OAUTH_ACCESS_TOKEN_SECRET_CONF, Type.PASSWORD, Importance.HIGH, TWITTER_OAUTH_ACCESS_TOKEN_SECRET_DOC, ACCOUNT_GROUP, 4, Width.LONG, TWITTER_OAUTH_ACCESS_TOKEN_SECRET_DISPLAY)
        .define(FILTER_KEYWORDS_CONF, Type.LIST, FILTER_KEYWORDS_DEFAULT, Importance.HIGH, FILTER_KEYWORDS_DOC, CONNECTOR_GROUP, 1, Width.MEDIUM, FILTER_KEYWORDS_DISPLAY)
        .define(KAFKA_STATUS_TOPIC_CONF, Type.STRING, Importance.HIGH, KAFKA_STATUS_TOPIC_DOC, CONNECTOR_GROUP, 2, Width.MEDIUM, KAFKA_STATUS_TOPIC_DISPLAY)
        .define(PROCESS_DELETES_CONF, Type.BOOLEAN, Importance.HIGH, PROCESS_DELETES_DOC, CONNECTOR_GROUP, 3, Width.MEDIUM, PROCESS_DELETES_DISPLAY)
        .define(KAFKA_DELETE_TOPIC_CONF, Type.STRING, Importance.HIGH, KAFKA_DELETE_TOPIC_DOC, CONNECTOR_GROUP, 4, Width.MEDIUM, KAFKA_DELETE_TOPIC_DISPLAY)
        .define(KAFKA_XFER_RATE_CONF, Type.INT, 0, Importance.MEDIUM, KAFKA_XFER_RATE_DOC, CONNECTOR_GROUP, 5, Width.SHORT, KAFKA_XFER_RATE_DISPLAY)
        .define(TWITTER_QUEUE_SIZE_CONF, Type.INT, 0, Importance.MEDIUM, TWITTER_QUEUE_SIZE_DOC, CONNECTOR_GROUP, 6, Width.SHORT, TWITTER_QUEUE_SIZE_DISPLAY)
        .define(TWITTER_DEBUG_CONF, Type.BOOLEAN, false, Importance.LOW, TWITTER_DEBUG_DOC, CONNECTOR_GROUP, 7, Width.SHORT, TWITTER_DEBUG_DISPLAY)
      ;
  }

  public static final ConfigDef CONFIG_DEF = conf();
  
  public Configuration configuration() {
    Properties properties = new Properties();
    /*
      Grab all of the key/values that have a key that starts with twitter. This will strip 'twitter.' from beginning of
      each key. This aligns with what the twitter4j framework is expecting.
     */
    properties.putAll(this.originalsWithPrefix("twitter."));
    return new PropertyConfiguration(properties);
  }

  public boolean twitterDebug() {
    return this.getBoolean(TWITTER_DEBUG_CONF);
  }

  public Integer kafkaXferRate() { 
    return this.getInt(KAFKA_XFER_RATE_CONF); 
  }

  public String[] filterKeywords() {
    List<String> keywordList = this.getList(FILTER_KEYWORDS_CONF);
    String[] keywords = Iterables.toArray(keywordList, String.class);
    return keywords;
  }

  public String kafkaStatusTopic() {
    return this.getString(KAFKA_STATUS_TOPIC_CONF);
  }

  public String kafkaDeleteTopic() {
    return this.getString(KAFKA_DELETE_TOPIC_CONF);
  }

  public boolean processDeletes() {
    return this.getBoolean(PROCESS_DELETES_CONF);
  }

  public Integer twitterQueueSize() { 
    return this.getInt(TWITTER_QUEUE_SIZE_CONF); 
  }
}
