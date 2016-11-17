package io.confluent.kafka.connect.twitter;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

/* Memory Usage considerations {based on testing in Nov 2016} */
/* Each tweet, with attendent schema, is about 10 KB when using the simple JSON converters */
/* Since we may not want to allocate multiple GB for just these tweets, we'll support   */
/* a max size for the queue (though the default will remain unlimited) */

public class TwitterSourceTask extends SourceTask implements StatusListener {
  static final Logger log = LoggerFactory.getLogger(TwitterSourceTask.class);
  final ConcurrentLinkedDeque<SourceRecord> messageQueue = new ConcurrentLinkedDeque<>();
  int maxMessageQueueSize = 0;

  TwitterStream twitterStream;
  RateLimiter rateLimiter = null;
  int maxRecordsPerPeriod = 0;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  TwitterSourceConnectorConfig config;

  @Override
  public void start(Map<String, String> map) {
    this.config = new TwitterSourceConnectorConfig(map);

    maxMessageQueueSize = this.config.twitterQueueSize();

      /* Define the rate limit based on 1-second sync using the RateLimiter */
      /* No limiter of XferRate == 0 */
    maxRecordsPerPeriod = this.config.kafkaXferRate();
    if (maxRecordsPerPeriod > 0)
      rateLimiter = RateLimiter.create(1.0);


    TwitterStreamFactory twitterStreamFactory = new TwitterStreamFactory(this.config.configuration());
    this.twitterStream = twitterStreamFactory.getInstance();

    String[] keywords = this.config.filterKeywords();

    if (log.isInfoEnabled()) {
      log.info("Setting up filters. Keywords = {}", Joiner.on(", ").join(keywords));
    }

    FilterQuery filterQuery = new FilterQuery();
    filterQuery.track(keywords);

    if (log.isInfoEnabled()) {
      log.info("Starting the twitter stream.");
    }
    twitterStream.addListener(this);
    twitterStream.filter(filterQuery);
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    List<SourceRecord> records = new ArrayList<>(256);
    int size = this.messageQueue.size();
    int throttledSize;

    if (rateLimiter != null)
      rateLimiter.acquire();

    if (maxRecordsPerPeriod > 0  &&  size > maxRecordsPerPeriod)
      throttledSize = maxRecordsPerPeriod;
    else
      throttledSize = size;

    if (size == 0)
      return (null);
    else
      log.debug("poll(): {} messages available messagQueue; throttling to {}.",
        Integer.toString(size), Integer.toString(throttledSize));

    for (int i = 0; i < throttledSize; i++) {
      SourceRecord record = this.messageQueue.poll();
      log.trace("record {} = {}", Integer.toString(i), record.toString());

      if (null == record) {
        break;
      }

      records.add(record);
    }

    log.debug("poll(): returning {} SourceRecords", Integer.toString(records.size()));
    return records;
  }

  @Override
  public void stop() {
    if (log.isInfoEnabled()) {
      log.info("Shutting down twitter stream.");
    }
    twitterStream.shutdown();
  }

  @Override
  public void onStatus(Status status) {
    if (maxMessageQueueSize > 0  &&  this.messageQueue.size() > maxMessageQueueSize) {
      log.info("onStatus(): Twitter messageQueue full ({} messages); discarding tweet", this.messageQueue.size());
      return;
    }

    try {
      Struct keyStruct = new Struct(StatusConverter.statusSchemaKey);
      Struct valueStruct = new Struct(StatusConverter.statusSchema);

      StatusConverter.convertKey(status, keyStruct);
      StatusConverter.convert(status, valueStruct);

      Map<String, ?> sourcePartition = ImmutableMap.of();
      Map<String, ?> sourceOffset = ImmutableMap.of();

      SourceRecord record = new SourceRecord(sourcePartition, sourceOffset, this.config.kafkaStatusTopic(), StatusConverter.statusSchemaKey, keyStruct, StatusConverter.statusSchema, valueStruct);
      this.messageQueue.add(record);
      log.debug("onStatus(): messageQueue.size() = {}", Integer.toString(this.messageQueue.size()));
    } catch (Exception ex) {
      if (log.isErrorEnabled()) {
        log.error("Exception thrown", ex);
      }
    }
  }

  @Override
  public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
    if (!this.config.processDeletes()) {
      return;
    }

    if (maxMessageQueueSize > 0  &&  this.messageQueue.size() > maxMessageQueueSize) {
      log.info("onDeletionNotice(): Twitter messageQueue full ({} messages); discarding delete notification", this.messageQueue.size());
      return;
    }

    try {
      Struct keyStruct = new Struct(StatusConverter.schemaStatusDeletionNoticeKey);
      Struct valueStruct = new Struct(StatusConverter.schemaStatusDeletionNotice);

      StatusConverter.convertKey(statusDeletionNotice, keyStruct);
      StatusConverter.convert(statusDeletionNotice, valueStruct);

      Map<String, ?> sourcePartition = ImmutableMap.of();
      Map<String, ?> sourceOffset = ImmutableMap.of();

      SourceRecord record = new SourceRecord(sourcePartition, sourceOffset, this.config.kafkaDeleteTopic(), StatusConverter.schemaStatusDeletionNoticeKey, keyStruct, StatusConverter.schemaStatusDeletionNotice, valueStruct);
      this.messageQueue.add(record);
      log.debug("onDeletionNotice(): messageQueue.size() = {}", Integer.toString(this.messageQueue.size()));
    } catch (Exception ex) {
      if (log.isErrorEnabled()) {
        log.error("Exception thrown", ex);
      }
    }
  }

  @Override
  public void onTrackLimitationNotice(int i) {

  }

  @Override
  public void onScrubGeo(long l, long l1) {

  }

  @Override
  public void onStallWarning(StallWarning stallWarning) {
    if (log.isWarnEnabled()) {
      log.warn("code = '{}' percentFull = '{}' - {}", stallWarning.getCode(), stallWarning.getPercentFull(), stallWarning.getMessage());
    }
  }

  @Override
  public void onException(Exception e) {
    if (log.isErrorEnabled()) {
      log.error("onException", e);
    }
  }
}
