package elastic.nativeapi;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.RestStatus;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

public class UpsertWithNative extends BaseTest {

    @Test
    public void upsertWhenObjectExistsTest() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("name", "Joe Smith")
                    .field("gender", "male")
                    .endObject();
            client.prepareIndex(index, type, id)
                    .setSource(builder)
                    .get();
            Thread.sleep(2000);
            IndexRequest indexRequest = new IndexRequest(index, type, id)
                    .source(XContentFactory.jsonBuilder()
                            .startObject()
                            .field("name", "Joe Smith")
                            .field("gender", "male")
                            .endObject());
            UpdateRequest updateRequest = new UpdateRequest(index, type, id)
                    .doc(XContentFactory.jsonBuilder()
                            .startObject()
                            .field("gender", "female")
                            .endObject())
                    .upsert(indexRequest);
            UpdateResponse updateResponse = client.update(updateRequest).get();
            assertNotNull(updateResponse);
            assertNotNull(updateResponse.status());
            assertEquals(RestStatus.OK, updateResponse.status());
        } catch (IOException | InterruptedException | ExecutionException e) {
            fail();
        } finally {
            client.prepareDelete(index, type, id).get();
        }
    }

    @Test
    public void upsertWhenObjectNotExistsTest() {
        try {
            IndexRequest indexRequest = new IndexRequest(index, type, id)
                    .source(XContentFactory.jsonBuilder()
                            .startObject()
                            .field("name", "Joe Smith")
                            .field("gender", "male")
                            .endObject());
            UpdateRequest updateRequest = new UpdateRequest(index, type, id)
                    .doc(XContentFactory.jsonBuilder()
                            .startObject()
                            .field("gender", "female")
                            .endObject())
                    .upsert(indexRequest);
            UpdateResponse updateResponse = client.update(updateRequest).get();
            assertNotNull(updateResponse);
            assertNotNull(updateResponse.status());
            assertEquals(RestStatus.CREATED, updateResponse.status());
        } catch (IOException | InterruptedException | ExecutionException e) {
            fail();
        } finally {
            client.prepareDelete(index, type, id).get();
        }
    }
}
