package elastic.nativeapi;

import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class BulkWIthNative extends BaseTest {

    @Test
    public void bulkApiTest() {
        try {
            BulkRequestBuilder bulkRequest = client.prepareBulk();
            bulkRequest.add(
                    client.prepareIndex(index, type, id)
                            .setSource(
                                    XContentFactory.jsonBuilder()
                                            .startObject()
                                            .field("user", "kimchy")
                                            .field("postDate", new Date())
                                            .field("message", "trying out Elasticsearch")
                                            .endObject()
                            )
            );

            bulkRequest.add(
                    client.prepareIndex(index, type, "2")
                            .setSource(
                                    XContentFactory.jsonBuilder()
                                            .startObject()
                                            .field("user", "kimchy")
                                            .field("postDate", new Date())
                                            .field("message", "trying out Elasticsearch")
                                            .endObject()
                            )
            );
            BulkResponse bulkResponse = bulkRequest.get();
            assertFalse(bulkResponse.hasFailures());
        } catch (IOException e) {
            fail();
        } finally {
            BulkRequestBuilder bulkRequest = client.prepareBulk();
            bulkRequest.add(client.prepareDelete(index, type, id));
            bulkRequest.add(client.prepareDelete(index, type, "2"));
            bulkRequest.get();
        }
    }

    @Test
    public void usingBulkProcessor() {
        BulkProcessor buildProcessor = null;
        try {
            buildProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
                @Override
                public void beforeBulk(long executionId, BulkRequest request) {

                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {

                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, Throwable failure) {

                }
            })
                    .setBulkActions(10000)
                    .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
                    .setFlushInterval(TimeValue.timeValueSeconds(5))
                    .setConcurrentRequests(1)
                    .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                    .build();
            buildProcessor.add(
                    new IndexRequest(index, type, id)
                    .source(XContentFactory.jsonBuilder()
                            .startObject()
                            .field("user", "kimchy")
                            .field("postDate", new Date())
                            .field("message", "trying out Elasticsearch")
                            .endObject())
            );
            buildProcessor.add(new DeleteRequest(index, type, id));
        } catch (IOException e) {
            fail();
        } finally {
            if (buildProcessor != null) {
                try {
                    buildProcessor.awaitClose(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    fail();
                }
            }
        }
    }
}
