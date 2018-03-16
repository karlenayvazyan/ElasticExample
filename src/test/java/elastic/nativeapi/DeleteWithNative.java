package elastic.nativeapi;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.rest.RestStatus;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;

import static org.junit.Assert.*;

public class DeleteWithNative extends BaseTest {

    @Test
    public void deleteTest() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("user", "kimchy")
                    .field("postDate", new Date())
                    .field("message", "trying out Elasticsearch")
                    .endObject();
            client.prepareIndex(index, type, id)
                    .setSource(builder)
                    .get();
            DeleteResponse deleteResponse = client.prepareDelete(index, type, id).get();
            assertNotNull(deleteResponse.status());
            assertEquals(RestStatus.OK, deleteResponse.status());
        } catch (IOException e) {
            fail();
        }
    }

    @Test
    public void deleteByQueryTest() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("user", "kimchy")
                    .field("postDate", new Date())
                    .field("message", "trying out Elasticsearch")
                    .endObject();
            client.prepareIndex(index, type, id)
                    .setSource(builder)
                    .get();
            Thread.sleep(2000);
            BulkByScrollResponse response = DeleteByQueryAction.INSTANCE
                    .newRequestBuilder(client)
                    .filter(QueryBuilders.matchQuery("user", "kimchy"))
                    .source(index)
                    .get();
            long deleted = response.getDeleted();
            assertEquals(1L, deleted);
        } catch (IOException | InterruptedException e) {
            fail();
        }
    }

    @Test
    public void deleteByQueryAsyncTest() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("user", "kimchy")
                    .field("postDate", new Date())
                    .field("message", "trying out Elasticsearch")
                    .endObject();
            client.prepareIndex(index, type, id)
                    .setSource(builder)
                    .get();
            Thread.sleep(2000);
            DeleteByQueryAction.INSTANCE
                    .newRequestBuilder(client)
                    .filter(QueryBuilders.matchQuery("user", "kimchy"))
                    .source("twitter8")
                    .execute(new ActionListener<BulkByScrollResponse>() {
                        @Override
                        public void onResponse(BulkByScrollResponse response) {
                            long deleted = response.getDeleted();
                            assertEquals(1L, deleted);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            fail();
                        }
                    });
        } catch (IOException | InterruptedException e) {
            fail();
        }
    }
}
