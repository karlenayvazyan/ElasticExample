package elastic.nativeapi;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class BaseTest {
    protected TransportClient client;

    protected String index = "twitter";
    protected String type = "tweet";
    protected String id = "1";

    @Before
    public void setUp() {
        try {
            client = new PreBuiltTransportClient(Settings.EMPTY)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
        } catch (UnknownHostException e) {
            throw new RuntimeException();
        }
    }
}
