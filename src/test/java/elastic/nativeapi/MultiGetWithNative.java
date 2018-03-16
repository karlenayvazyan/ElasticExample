package elastic.nativeapi;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.junit.Test;

public class MultiGetWithNative extends BaseTest {

    @Test
    public void multiGetTest() {
        String index = "bank";
        String type = "_doc";
        MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
                .add(index, type, "0")
                .add(index, type, "1", "2", "3")
                .get();
        for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
            GetResponse response = itemResponse.getResponse();
            if (response.isExists()) {
                String json = response.getSourceAsString();
                System.out.println(json);
            }
        }
    }
}
