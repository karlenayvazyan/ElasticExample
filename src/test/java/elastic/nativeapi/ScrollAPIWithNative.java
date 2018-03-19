package elastic.nativeapi;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;

public class ScrollAPIWithNative extends BaseTest {

    @Test
    public void testScrollApi() {

        SearchResponse searchResponse = client.prepareSearch("bank", "twitter")
                .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                .setScroll(new TimeValue(60000))
                .setSize(100)
                .get();

        do {
            for (SearchHit documentFields : searchResponse.getHits().getHits()) {
                System.out.println(documentFields.getSourceAsMap());
            }
            searchResponse = client.prepareSearchScroll(searchResponse.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
            System.out.println("-------------------------------");
        } while (searchResponse.getHits().getHits().length != 0);
    }
}
