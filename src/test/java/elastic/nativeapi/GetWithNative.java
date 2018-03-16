package elastic.nativeapi;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GetWithNative extends BaseTest {

    @Test
    public void getTest() {
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
            GetResponse documentFields = client.prepareGet(index, type, id).get();
            assertTrue(documentFields.isExists());
        } catch (IOException e) {
            fail();
        } finally {
            client.prepareDelete(index, type, id).get();
        }
    }
}
