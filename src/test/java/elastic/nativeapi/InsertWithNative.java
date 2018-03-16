package elastic.nativeapi;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class InsertWithNative extends BaseTest {

    @Test
    public void createIndexUsingJsonString() {
        try {
            String json = "{" +
                    "\"user\":\"kimchy\"," +
                    "\"postDate\":\"2013-01-30\"," +
                    "\"message\":\"trying out Elasticsearch\"" +
                    "}";
            IndexResponse indexResponse = client.prepareIndex(index, type, id)
                    .setSource(json, XContentType.JSON)
                    .get();
            assertNotNull(indexResponse);
            assertNotNull(indexResponse.status());
            assertEquals(RestStatus.OK, indexResponse.status());
        } finally {
            client.prepareDelete(index, type, id).get();
        }
    }

    @Test
    public void createIndexUsingMap() {
        try {
            Map<String, Object> json = new HashMap<>();
            json.put("user", "kimchy");
            json.put("postDate", new Date());
            json.put("message", "trying out Elasticsearch");
            IndexResponse indexResponse = client.prepareIndex(index, type, id)
                    .setSource(json)
                    .get();
            assertNotNull(indexResponse);
            assertNotNull(indexResponse.status());
            assertEquals(RestStatus.CREATED, indexResponse.status());
        } finally {
            client.prepareDelete(index, type, id).get();
        }
    }

    @Test
    public void createIndexUsingXContentBuilder() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("user", "kimchy")
                    .field("postDate", new Date())
                    .field("message", "trying out Elasticsearch")
                    .endObject();
            IndexResponse indexResponse = client.prepareIndex(index, type, id)
                    .setSource(builder)
                    .get();
            assertNotNull(indexResponse);
            assertNotNull(indexResponse.status());
            assertEquals(RestStatus.CREATED, indexResponse.status());
        } catch (IOException e) {
            client.prepareDelete(index, type, id).get();
        }
    }
}
