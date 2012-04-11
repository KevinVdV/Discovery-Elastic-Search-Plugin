package org.dspace.discovery;

import org.apache.log4j.Logger;
import org.dspace.content.DCDate;
import org.dspace.content.DSpaceObject;
import org.dspace.core.Constants;
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
import java.util.Date;
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

    private static Client client;
    private static final Logger log = Logger.getLogger(ElasticSearchSearcher.class);

    @Override
    public DiscoverResult search(Context context, DSpaceObject dso, DiscoverQuery query) throws SearchServiceException {
        //TODO: Add a location query ?
        return search(context, query);
    }

    @Override
    public DiscoverResult search(Context context, DiscoverQuery query) throws SearchServiceException {
        try {
            SearchRequestBuilder requestBuilder = getClient().prepareSearch("discovery-index")
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setQuery(query.getQuery())
                    .setFrom(query.getStart()).setSize(query.getMaxResults()).setExplain(true);

            for (int i = 0; i < query.getFilterQueries().size(); i++) {
                String filterQuery = query.getFilterQueries().get(i);
                requestBuilder.setFilter(filterQuery);

            }

            //Add our facets to the query
            List<DiscoverFacetField> facetFields = query.getFacetFields();
            for (DiscoverFacetField discoverFacetField : facetFields) {
                TermsFacetBuilder termsFacetBuilder = new TermsFacetBuilder(discoverFacetField.getField());
                termsFacetBuilder.size(discoverFacetField.getLimit());
                termsFacetBuilder.field(discoverFacetField.getField() + "_filter");
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
                            facetResults.add(new DiscoverResult.FacetResult(discoverFacetField.getField() + ":" + entry.getTerm(), entry.getTerm(), entry.getCount()));
                        }
                        result.addFacetResult(discoverFacetField.getField(), facetResults.toArray(new DiscoverResult.FacetResult[facetResults.size()]));
                    }

                }

            }
            return result;
        } catch (SQLException e) {
            throw new SearchServiceException(e.getMessage(), e);
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
        DiscoverFilterQuery result = new DiscoverFilterQuery();

        String field = filterQuery;
        String value = filterQuery;

        if(filterQuery.contains(":"))
        {
            field = filterQuery.substring(0, filterQuery.indexOf(":"));
            value = filterQuery.substring(filterQuery.indexOf(":") + 1, filterQuery.length());
        }else{
            //We have got no field, so we are using everything
            field = "*";
        }

        value = value.replace("\\", "");
        if("*".equals(field))
        {
            field = "all";
        }
        if(filterQuery.startsWith("*:") || filterQuery.startsWith(":"))
        {
            filterQuery = filterQuery.substring(filterQuery.indexOf(":") + 1, filterQuery.length());
        }
//        "(tea coffee)".replaceFirst("\\((.*?)\\)", "")

        value = transformDisplayedValue(context, field, value);

        result.setField(field);
        result.setFilterQuery(filterQuery);
        result.setDisplayedValue(value);
        return result;
    }


    private String transformDisplayedValue(Context context, String field, String value) throws SQLException {
        if(field.equals("location.comm") || field.equals("location.coll")){
            value = locationToName(context, field, value);
        }else if(value.matches("\\((.*?)\\)"))
        {
            //The brackets where added for better solr results, remove the first & last one
            value = value.substring(1, value.length() -1);
        }
        return value;
    }

    public static String locationToName(Context context, String field, String value) throws SQLException {
        if("location.comm".equals(field) || "location.coll".equals(field)){
            int type = field.equals("location.comm") ? Constants.COMMUNITY : Constants.COLLECTION;
            DSpaceObject commColl = DSpaceObject.find(context, type, Integer.parseInt(value));
            if(commColl != null)
            {
                return commColl.getName();
            }

        }
        return value;
    }


    @Override
    public DiscoverFilterQuery toFilterQuery(Context context, String field, String value) throws SQLException {
        DiscoverFilterQuery result = new DiscoverFilterQuery();
        result.setField(field);
        result.setDisplayedValue(transformDisplayedValue(context, field, value));
        result.setFilterQuery((field == null || field.equals("") ? "" : field + ":") +  "(" + value + ")");
        return result;
    }

    @Override
    public String toSortFieldIndex(String metadataField, String type) {
        return metadataField;
    }

    protected Client getClient(){
        if(client == null) {
            Node node = NodeBuilder.nodeBuilder().node();
            client = node.client();
//            node.close();
        }
        return client;
    }
}