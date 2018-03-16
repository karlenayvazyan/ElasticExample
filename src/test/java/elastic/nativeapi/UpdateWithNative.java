package elastic.nativeapi;

import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

public class UpdateWithNative extends BaseTest {

    @Test
    public void updateRequestTest() {
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
            UpdateRequest updateRequest = new UpdateRequest();
            updateRequest.index(index);
            updateRequest.type(type);
            updateRequest.id(id);
            updateRequest.doc(XContentFactory.jsonBuilder()
                    .startObject()
                    .field("user", "kimchy78")
                    .endObject());
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
    public void prepareUpdateScriptTest() {
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
            UpdateResponse updateResponse = client.prepareUpdate(index, type, id)
                    .setScript(new Script("ctx._source.user = \"kimchy\""))
                    .get();
            assertNotNull(updateResponse);
            assertNotNull(updateResponse.status());
            assertEquals(RestStatus.OK, updateResponse.status());
        } catch (IOException | InterruptedException e) {
            fail();
        } finally {
            client.prepareDelete(index, type, id).get();
        }
    }

    @Test
    public void prepareUpdateJsonBuilderTest() {
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
            UpdateResponse updateResponse = client.prepareUpdate(index, type, id)
                    .setDoc(XContentFactory.jsonBuilder()
                            .startObject()
                            .field("user", "kimchy9000")
                            .endObject())
                    .get();
            assertNotNull(updateResponse);
            assertNotNull(updateResponse.status());
            assertEquals(RestStatus.OK, updateResponse.status());
        } catch (IOException | InterruptedException e) {
            fail();
        } finally {
            client.prepareDelete(index, type, id).get();
        }
    }

    @Test
    public void updateMergingDocumentTest() {
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
            UpdateResponse updateResponse = client.prepareUpdate(index, type, id)
                    .setDoc(XContentFactory.jsonBuilder()
                            .startObject()
                            .field("gender", "male")
                            .endObject())
                    .get();
            assertNotNull(updateResponse);
            assertNotNull(updateResponse.status());
            assertEquals(RestStatus.OK, updateResponse.status());
        } catch (IOException | InterruptedException e) {
            fail();
        } finally {
            client.prepareDelete(index, type, id).get();
        }
    }
}
