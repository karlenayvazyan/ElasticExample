package elastic.nativeapi;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class SearchWithNative extends BaseTest {

    @Test
    public void test() {
        SearchResponse searchResponse = client.prepareSearch(index, "bank")
                .setTypes("_doc", type)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.matchQuery("firstname", "Virginia"))
                .setPostFilter(QueryBuilders.rangeQuery("age").from(28).to(40))
                .setFrom(0).setSize(60).setExplain(true)
                .get();
        assertNotNull(searchResponse);
        assertNotNull(searchResponse.getHits());
        System.out.println(searchResponse.getHits().totalHits);
    }
}