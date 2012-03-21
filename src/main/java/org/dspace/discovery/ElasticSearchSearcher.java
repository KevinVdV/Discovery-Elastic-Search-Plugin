package org.dspace.discovery;

import org.apache.log4j.Logger;
import org.dspace.content.DSpaceObject;
import org.dspace.core.Context;
import org.dspace.core.LogManager;
import org.dspace.discovery.*;
import org.dspace.discovery.configuration.DiscoveryConfigurationParameters;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.action.search.SearchRequestBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.facet.AbstractFacetBuilder;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.facet.Facets;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.facet.terms.TermsFacetBuilder;

import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: Kevin
 * Date: 3/03/12
 * Time: 15:36
 * To change this template use File | Settings | File Templates.
 */
public class ElasticSearchSearcher implements SearchService {

    private static final Logger log = Logger.getLogger(ElasticSearchSearcher.class);

    @Override
    public DiscoverResult search(Context context, DSpaceObject dso, DiscoverQuery query) throws SearchServiceException {
        //TODO: Add a location query ?
        return search(context, query);
    }

    @Override
    public DiscoverResult search(Context context, DiscoverQuery query) throws SearchServiceException {
        Node node = NodeBuilder.nodeBuilder().node();
        try {
            Client client = node.client();
            SearchRequestBuilder requestBuilder = client.prepareSearch("repository")
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setQuery(query.getQuery())
                    .setFrom(query.getStart()).setSize(query.getMaxResults()).setExplain(true);

            //Add our facets to the query
            List<DiscoverFacetField> facetFields = query.getFacetFields();
            for (DiscoverFacetField discoverFacetField : facetFields) {
                TermsFacetBuilder termsFacetBuilder = new TermsFacetBuilder(discoverFacetField.getField());
                termsFacetBuilder.size(discoverFacetField.getLimit());
                if(discoverFacetField.getSortOrder().equals(DiscoveryConfigurationParameters.SORT.COUNT)){
                    termsFacetBuilder.order(TermsFacet.ComparatorType.COUNT);
                }else{
                    termsFacetBuilder.order(TermsFacet.ComparatorType.TERM);
                }
                requestBuilder.addFacet(termsFacetBuilder);
            }

            SearchResponse response = requestBuilder
                    .execute()
                    .actionGet();


            SearchHits hits = response.getHits();

            DiscoverResult result = new DiscoverResult();
            if(hits != null){
                result.setStart(query.getStart());
                result.setMaxResults(query.getMaxResults());
                result.setTotalSearchResults(response.getHits().getTotalHits());

                for (int i = 0; i < hits.getHits().length; i++) {
                    SearchHit searchHit = hits.getHits()[i];
                    DSpaceObject dso = findDSpaceObject(context, searchHit);

                    if(dso != null){
                        result.addDSpaceObject(dso);
                    } else {
                        log.error(LogManager.getHeader(context, "Error while retrieving DSpace object from discovery index", "Id: " + searchHit.getId() + " type " + searchHit.getType()));
                    }
                }

                Facets facets = response.getFacets();
                if(facets != null){
                    Map<String,Facet> facetMap = facets.getFacets();
                    for (DiscoverFacetField discoverFacetField : facetFields) {
                        TermsFacet resultFacet = (TermsFacet) facetMap.get(discoverFacetField.getField());
                        List<DiscoverResult.FacetResult> facetResults = new ArrayList<DiscoverResult.FacetResult>();
                        List<? extends TermsFacet.Entry> entries = resultFacet.getEntries();
                        for(TermsFacet.Entry entry : entries){
                            //TODO: as fitler query !
                            facetResults.add(new DiscoverResult.FacetResult("", entry.getTerm(), entry.getCount()));
                        }
                        result.addFacetResult(discoverFacetField.getField(), facetResults.toArray(new DiscoverResult.FacetResult[facetResults.size()]));
                    }

                }

            }


            return result;
        } catch (SQLException e) {
            throw new SearchServiceException(e.getMessage(), e);
        } finally {
            if(node != null){
                node.close();
            }
        }
    }

    private DSpaceObject findDSpaceObject(Context context, SearchHit searchHit) throws SQLException {
        return DSpaceObject.find(context, Integer.parseInt(searchHit.getType()), Integer.parseInt(searchHit.getId()));
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
