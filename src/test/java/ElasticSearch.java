import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.junit.Assert.assertTrue;

public class ElasticSearch {

    private TransportClient client = null;

    @Before
    public void setUp() {
        try {
            this.client = transportClient();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    private static TransportClient transportClient() throws UnknownHostException {
        return new PreBuiltTransportClient(
                Settings.builder()
                        .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), "elasticsearch")
                        .build()
        ).addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
    }

    @Test
    public void healthTest_1() {
        ClusterHealthResponse response = client.admin()
                .cluster()
                .health(
                        Requests
                                .clusterHealthRequest()
                                .waitForGreenStatus()
                                .timeout(TimeValue.timeValueSeconds(5))
                )
                .actionGet();

        assertTrue(response.isTimedOut());
    }

    @Test
    public void healthTest_2() {
        ClusterHealthResponse clusterHealthResponse = client.admin()
                .cluster()
                .prepareHealth()
                .setWaitForGreenStatus()
                .setTimeout(TimeValue.timeValueSeconds(5))
                .execute()
                .actionGet();

        assertTrue(clusterHealthResponse.isTimedOut());
    }

    @Test
    public void indexesExists() {
        IndicesExistsResponse response = client.admin()
                .indices()
                .prepareExists("bank")
                .get(TimeValue.timeValueMillis(100));
        assertTrue(response.isExists());
    }

    @Test
    @Ignore
    public void createNewIndex() {
        try (final ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("account.json");
            Streams.copy(inputStream, outputStream);
            CreateIndexResponse catalog = client.admin()
                    .indices()
                    .prepareCreate("catalog")
                    .setSource(outputStream.toByteArray(), XContentType.JSON)
                    .setTimeout(TimeValue.timeValueSeconds(1))
                    .get(TimeValue.timeValueSeconds(2));
            System.out.println(catalog.isAcknowledged());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void givenJsonString_whenJavaObject_thenIndexDocument() {
        String jsonObject = "{\"age\":10,\"dateOfBirth\":1471466076564,"
                +"\"fullName\":\"John Doe\"}";
        IndexResponse response = client.prepareIndex("people", "Doe")
                .setSource(jsonObject).get();
        System.out.println(response.status().getStatus());
    }
}
