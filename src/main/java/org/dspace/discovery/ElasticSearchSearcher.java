package org.dspace.discovery;

import org.dspace.content.DSpaceObject;
import org.dspace.core.Context;
import org.dspace.discovery.*;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHits;

import java.io.InputStream;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Kevin
 * Date: 3/03/12
 * Time: 15:36
 * To change this template use File | Settings | File Templates.
 */
public class ElasticSearchSearcher implements SearchService {
    @Override
    public DiscoverResult search(Context context, DSpaceObject dso, DiscoverQuery query) throws SearchServiceException {
        //TODO: Add a location query ?
        return search(context, query);
    }

    @Override
    public DiscoverResult search(Context context, DiscoverQuery query) throws SearchServiceException {
        Node node = NodeBuilder.nodeBuilder().node();
        Client client = node.client();
        SearchResponse response = client.prepareSearch("repository")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(query.getQuery())
                .setFrom(query.getStart()).setSize(query.getMaxResults()).setExplain(true)
                .execute()
                .actionGet();

        SearchHits hits = response.getHits();
        System.out.println("TOTAL HITS: " + hits.getHits().length);

        return null;
    }

    @Override
    public InputStream searchJSON(DiscoverQuery query, String jsonIdentifier) throws SearchServiceException {
        return null;
    }

    @Override
    public InputStream searchJSON(DiscoverQuery query, DSpaceObject dso, String jsonIdentifier) throws SearchServiceException {
        return null;
    }

    @Override
    public List<DSpaceObject> search(Context context, String query, String orderfield, boolean ascending, int offset, int max, String... filterquery) {
        return null;
    }

    @Override
    public DiscoverFilterQuery toFilterQuery(Context context, String filterQuery) throws SQLException {
        return null;
    }

    @Override
    public DiscoverFilterQuery toFilterQuery(Context context, String field, String value) throws SQLException {
        return null;
    }

    @Override
    public String toSortFieldIndex(String metadataField, String type) {
        return metadataField;
    }
}
