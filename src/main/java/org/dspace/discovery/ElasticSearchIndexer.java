package org.dspace.discovery;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.log4j.Logger;
import org.dspace.content.*;
import org.dspace.content.Collection;
import org.dspace.core.ConfigurationManager;
import org.dspace.core.Constants;
import org.dspace.core.Context;
import org.dspace.discovery.configuration.*;
import org.dspace.handle.HandleManager;
import org.dspace.utils.DSpace;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.action.index.IndexRequestBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Kevin
 * Date: 3/03/12
 * Time: 15:36
 * To change this template use File | Settings | File Templates.
 */
public class ElasticSearchIndexer implements IndexingService{

    private static Client indexClient;
    public static final String INDEX_NAME = "discovery-index";

    //TODO: perhaps not needed for elastic search ?
    public static final String FILTER_SEPARATOR = "|||";


    private static final Logger log = Logger.getLogger(ElasticSearchIndexer.class);

    @Override
    public void indexContent(Context context, DSpaceObject dso) throws SQLException {

    }

    @Override
    public void indexContent(Context context, DSpaceObject dso, boolean force) throws SQLException {
        String handle = dso.getHandle();

        if (handle == null) {
            handle = HandleManager.findHandle(context, dso);
        }

        Client client = getIndexClient();
        IndexRequestBuilder dspaceObjectBuilder = client.prepareIndex(INDEX_NAME, String.valueOf(dso.getType()), String.valueOf(dso.getID()));

        try {
            switch (dso.getType()) {
                case Constants.ITEM:
                    Item item = (Item) dso;
                    if (item.isArchived() && !item.isWithdrawn()) {
                        /**
                         * If the item is in the repository now, add it to the index
                         */
                        //TODO: DO REQUIRES INDEXING !
//                        if (requiresIndexing(handle, ((Item) dso).getLastModified())
//                                || force) {
                        unIndexContent(context, handle);
                        buildDocument(context, (Item) dso, dspaceObjectBuilder);
//                        }
                    } else {
                        /**
                         * Make sure the item is not in the index if it is not in
                         * archive.
                         * content on search/retrieval and allow admins the ability
                         * to still search for withdrawn Items.
                         */
                        unIndexContent(context, handle);
                        log.info("Removed Item: " + handle + " from Index");
                    }
                    break;

                case Constants.COLLECTION:
//                    buildDocument((Collection) dso);
                    log.info("Wrote Collection: " + handle + " to Index");
                    break;

                case Constants.COMMUNITY:
//                    buildDocument((Community) dso);
                    log.info("Wrote Community: " + handle + " to Index");
                    break;

                default:
                    log
                            .error("Only Items, Collections and Communities can be Indexed");
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            e.printStackTrace();
            System.exit(1);
        }
        //Perform the action!
        client.close();

    }

    private void buildDocument(Context context, Item item, IndexRequestBuilder repository) throws IOException, SQLException {
        String handle = item.getHandle();

        if (handle == null) {
            handle = HandleManager.findHandle(context, item);
        }

        // get the location string (for searching by collection & community)
        List<String> locations = getItemLocations(item);


        XContentBuilder itemBuilder = XContentFactory.jsonBuilder().startObject();

        List<String> sortFieldsAdded = new ArrayList<String>();
        try {
            List<DiscoveryConfiguration> discoveryConfigurations = SearchUtils.getAllDiscoveryConfigurations(item);

            //A map used to save each sidebarFacet config by the metadata fields
            Map<String, List<SidebarFacetConfiguration>> sidebarFacets = new HashMap<String, List<SidebarFacetConfiguration>>();
            Map<String, List<DiscoverySearchFilter>> searchFilters = new HashMap<String, List<DiscoverySearchFilter>>();
            Map<String, DiscoverySortFieldConfiguration> sortFields = new HashMap<String, DiscoverySortFieldConfiguration>();
            Map<String, DiscoveryRecentSubmissionsConfiguration> recentSubmissionsConfigurationMap = new HashMap<String, DiscoveryRecentSubmissionsConfiguration>();
            for (DiscoveryConfiguration discoveryConfiguration : discoveryConfigurations) {
                //Sidebar facet mapping configuration read in
                for(SidebarFacetConfiguration facet : discoveryConfiguration.getSidebarFacets()){
                    for (int i = 0; i < facet.getMetadataFields().size(); i++) {
                        String metadataField = facet.getMetadataFields().get(i);
                        List<SidebarFacetConfiguration> resultingList;
                        if(sidebarFacets.get(metadataField) != null){
                            resultingList = sidebarFacets.get(metadataField);
                        }else{
                            //New metadata field, create a new list for it
                            resultingList = new ArrayList<SidebarFacetConfiguration>();
                        }
                        resultingList.add(facet);

                        sidebarFacets.put(metadataField, resultingList);
                    }
                }

                for (int i = 0; i < discoveryConfiguration.getSearchFilters().size(); i++) {
                    DiscoverySearchFilter discoverySearchFilter = discoveryConfiguration.getSearchFilters().get(i);
                    for (int j = 0; j < discoverySearchFilter.getMetadataFields().size(); j++) {
                        String metadataField = discoverySearchFilter.getMetadataFields().get(j);
                        List<DiscoverySearchFilter> resultingList;
                        if(searchFilters.get(metadataField) != null){
                            resultingList = searchFilters.get(metadataField);
                        }else{
                            //New metadata field, create a new list for it
                            resultingList = new ArrayList<DiscoverySearchFilter>();
                        }
                        resultingList.add(discoverySearchFilter);

                        searchFilters.put(metadataField, resultingList);
                    }
                }

                DiscoverySortConfiguration sortConfiguration = discoveryConfiguration.getSearchSortConfiguration();
                if(sortConfiguration != null){
                    for (DiscoverySortFieldConfiguration discoverySortConfiguration : sortConfiguration.getSortFields()) {
                        sortFields.put(discoverySortConfiguration.getMetadataField(), discoverySortConfiguration);
                    }
                }

                DiscoveryRecentSubmissionsConfiguration recentSubmissionConfiguration = discoveryConfiguration.getRecentSubmissionConfiguration();
                if(recentSubmissionConfiguration != null){
                    recentSubmissionsConfigurationMap.put(recentSubmissionConfiguration.getMetadataSortField(), recentSubmissionConfiguration);
                }
            }

            List<String> toIgnoreFields = new ArrayList<String>();
            String ignoreFieldsString = new DSpace().getConfigurationService().getProperty("discovery.index.ignore");
            if(ignoreFieldsString != null){
                if(ignoreFieldsString.indexOf(",") != -1){
                    for (int i = 0; i < ignoreFieldsString.split(",").length; i++) {
                        toIgnoreFields.add(ignoreFieldsString.split(",")[i].trim());
                    }
                } else {
                    toIgnoreFields.add(ignoreFieldsString);
                }
            }
            DCValue[] mydc = item.getMetadata(Item.ANY, Item.ANY, Item.ANY, Item.ANY);
            for (DCValue meta : mydc){
                String field = meta.schema + "." + meta.element;
                String unqualifiedField = field;

                String value = meta.value;

                if (value == null) {
                    continue;
                }

                if (meta.qualifier != null && !meta.qualifier.trim().equals("")) {
                    field += "." + meta.qualifier;
                }


                //We are not indexing provenance, this is useless
                if (toIgnoreFields.contains(field) || toIgnoreFields.contains(unqualifiedField + "." + Item.ANY)) {
                    continue;
                }

                if ((searchFilters.get(field) != null || searchFilters.get(unqualifiedField + "." + Item.ANY) != null)) {
                    List<DiscoverySearchFilter> searchFilterConfigs = searchFilters.get(field);
                    if(searchFilterConfigs == null){
                        searchFilterConfigs = searchFilters.get(unqualifiedField + "." + Item.ANY);
                    }

                    for (DiscoverySearchFilter searchFilter : searchFilterConfigs) {
                        if(searchFilter.getType().equals(DiscoveryConfigurationParameters.TYPE_DATE)){
                            //For our search filters that are dates we format them properly
                            Date date = toDate(value);
                            if(date != null){
                                //TODO: make this date format configurable !
                                value = DateFormatUtils.formatUTC(date, "yyyy-MM-dd");
                            }
                        }

                        itemBuilder.field(searchFilter.getIndexFieldName(), value);
                        //Add a dynamic fields for auto complete in search
                        if(searchFilter.isFullAutoComplete()){
                            itemBuilder.field(searchFilter.getIndexFieldName() + "_ac", value);
                        }else{
                            String[] values = value.split(" ");
                            for (String val : values) {
                                itemBuilder.field(searchFilter.getIndexFieldName() + "_ac", val);
                            }
                        }
                    }
                }

                if (sidebarFacets.get(field) != null || sidebarFacets.get(unqualifiedField + "." + Item.ANY) != null) {
                    //Retrieve the configurations
                    List<SidebarFacetConfiguration> facetConfigurations = sidebarFacets.get(field);
                    if(facetConfigurations == null){
                        facetConfigurations = sidebarFacets.get(unqualifiedField + "." + Item.ANY);
                    }

                    for (SidebarFacetConfiguration configuration : facetConfigurations) {
                        if(configuration.getType().equals(DiscoveryConfigurationParameters.TYPE_TEXT)){
                            //Add a special filter
                            //We use a separator to split up the lowercase and regular case, this is needed to get our filters in regular case
                            //Solr has issues with facet prefix and cases
                            String separator = new DSpace().getConfigurationService().getProperty("discovery.solr.facets.split.char");
                            if(separator == null){
                                separator = FILTER_SEPARATOR;
                            }
                            itemBuilder.field(configuration.getIndexFieldName() + "_filter", value.toLowerCase() + separator + value);
                        }else
                        if(configuration.getType().equals(DiscoveryConfigurationParameters.TYPE_DATE)){
                            //For our sidebar filters that are dates we only add the year
                            Date date = toDate(value);
                            if(date != null){
                                String indexField = configuration.getIndexFieldName() + ".year";
                                itemBuilder.field(indexField, DateFormatUtils.formatUTC(date, "yyyy"));
                                //Also save a sort value of this year, this is required for determining the upper & lower bound year of our facet
                                if(itemBuilder.field(indexField + "_sort") == null){
                                    //We can only add one year so take the first one
                                    itemBuilder.field(indexField + "_sort", DateFormatUtils.formatUTC(date, "yyyy"));
                                }
                            } else {
                                log.warn("Error while indexing sidebar date field, item: " + item.getHandle() + " metadata field: " + field + " date value: " + date);
                            }
                        }
                    }
                }

                if ((sortFields.get(field) != null || recentSubmissionsConfigurationMap.get(field) != null) && !sortFieldsAdded.contains(field)) {
                    //Only add sort value once
                    String type;
                    if(sortFields.get(field) != null){
                        type = sortFields.get(field).getType();
                    }else{
                        type = recentSubmissionsConfigurationMap.get(field).getType();
                    }

                    if(type.equals(DiscoveryConfigurationParameters.TYPE_DATE)){
                        Date date = toDate(value);
                        if(date != null){
                            itemBuilder.field(field + "_dt", date);
                        }else{
                            log.warn("Error while indexing sort date field, item: " + item.getHandle() + " metadata field: " + field + " date value: " + date);
                        }
                    }else{
                        itemBuilder.field(field + "_sort", value);
                    }
                    sortFieldsAdded.add(field);
                }

                itemBuilder.field(field, value.toLowerCase());

                if (meta.language != null && !meta.language.trim().equals("")) {
                    String langField = field + "." + meta.language;
                    itemBuilder.field(langField, value);
                }
            }

        } catch (Exception e)  {
            log.error(e.getMessage(), e);
        }


        log.debug("  Added Metadata");

        try {

            DCValue[] values = item.getMetadata("dc.relation.ispartof");

            if(values != null && values.length > 0 && values[0] != null && values[0].value != null)
            {
                // group on parent
                String handlePrefix = ConfigurationManager.getProperty("handle.canonical.prefix");
                if (handlePrefix == null || handlePrefix.length() == 0)
                {
                    handlePrefix = "http://hdl.handle.net/";
                }

                itemBuilder.field("publication_grp", values[0].value.replaceFirst(handlePrefix, ""));

            }
            else
            {
                // group on self
                itemBuilder.field("publication_grp", item.getHandle());
            }

        } catch (Exception e){
            log.error(e.getMessage(),e);
        }


        log.debug("  Added Grouping");


        Vector<InputStreamReader> readers = new Vector<InputStreamReader>();

        try {
            // now get full text of any bitstreams in the TEXT bundle
            // trundle through the bundles
            Bundle[] myBundles = item.getBundles();

            for (Bundle myBundle : myBundles) {
                if ((myBundle.getName() != null)
                        && myBundle.getName().equals("TEXT")) {
                    // a-ha! grab the text out of the bitstreams
                    Bitstream[] myBitstreams = myBundle.getBitstreams();

                    for (Bitstream myBitstream : myBitstreams) {
                        try {
                            InputStreamReader is = new InputStreamReader(
                                    myBitstream.retrieve()); // get input
                            readers.add(is);

                            // Add each InputStream to the Indexed Document
                            itemBuilder.field("fulltext", IOUtils.toString(is));

                            log.debug("  Added BitStream: "
                                    + myBitstream.getStoreNumber() + "	"
                                    + myBitstream.getSequenceID() + "   "
                                    + myBitstream.getName());

                        } catch (Exception e) {
                            // this will never happen, but compiler is now
                            // happy.
                            log.trace(e.getMessage(), e);
                        }
                    }
                }
            }
        } catch (RuntimeException e) {
            log.error(e.getMessage(), e);
        }

        // write the index and close the inputstreamreaders
        try {
            byte[] source = itemBuilder.endObject().copiedBytes();
            repository.setSource(source);
            repository.execute().actionGet();
            log.info("Wrote Item: " + handle + " to Index");
        } catch (RuntimeException e) {
            log.error("Error while writing item to discovery index: " + handle + " message:"+ e.getMessage(), e);
        } finally {
            Iterator<InputStreamReader> itr = readers.iterator();
            while (itr.hasNext()) {
                InputStreamReader reader = itr.next();
                if (reader != null) {
                    reader.close();
                }
            }
            log.debug("closed " + readers.size() + " readers");
        }
    }

    /**
     * @param myitem the item for which our locations are to be retrieved
     * @return a list containing the identifiers of the communities & collections
     * @throws SQLException sql exception
     */
    private List<String> getItemLocations(Item myitem)
            throws SQLException {
        List<String> locations = new Vector<String>();

        // build list of community ids
        Community[] communities = myitem.getCommunities();

        // build list of collection ids
        Collection[] collections = myitem.getCollections();

        // now put those into strings
        int i = 0;

        for (i = 0; i < communities.length; i++)
        {
            locations.add("m" + communities[i].getID());
        }

        for (i = 0; i < collections.length; i++)
        {
            locations.add("l" + collections[i].getID());
        }

        return locations;
    }

    /**
     * Helper function to retrieve a date using a best guess of the potential
     * date encodings on a field
     *
     * @param t the string to be transformed to a date
     * @return a date if the formatting was successful, null if not able to transform to a date
     */
    public static Date toDate(String t) {
        SimpleDateFormat[] dfArr;

        // Choose the likely date formats based on string length
        switch (t.length()) {
            case 4:
                dfArr = new SimpleDateFormat[]{new SimpleDateFormat("yyyy")};
                break;
            case 6:
                dfArr = new SimpleDateFormat[]{new SimpleDateFormat("yyyyMM")};
                break;
            case 7:
                dfArr = new SimpleDateFormat[]{new SimpleDateFormat("yyyy-MM")};
                break;
            case 8:
                dfArr = new SimpleDateFormat[]{new SimpleDateFormat("yyyyMMdd"),
                        new SimpleDateFormat("yyyy MMM")};
                break;
            case 10:
                dfArr = new SimpleDateFormat[]{new SimpleDateFormat("yyyy-MM-dd")};
                break;
            case 11:
                dfArr = new SimpleDateFormat[]{new SimpleDateFormat("yyyy MMM dd")};
                break;
            case 20:
                dfArr = new SimpleDateFormat[]{new SimpleDateFormat(
                        "yyyy-MM-dd'T'HH:mm:ss'Z'")};
                break;
            default:
                dfArr = new SimpleDateFormat[]{new SimpleDateFormat(
                        "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")};
                break;
        }

        for (SimpleDateFormat df : dfArr) {
            try {
                // Parse the date
                df.setCalendar(Calendar
                        .getInstance(TimeZone.getTimeZone("UTC")));
                df.setLenient(false);
                return df.parse(t);
            } catch (ParseException pe) {
                log.error("Unable to parse date format", pe);
            }
        }

        return null;
    }

    @Override
    public void unIndexContent(Context context, DSpaceObject dso) throws SQLException, IOException {

    }

    @Override
    public void unIndexContent(Context context, String handle) throws SQLException, IOException {

    }

    @Override
    public void unIndexContent(Context context, String handle, boolean commit) throws SQLException, IOException {

    }

    @Override
    public void reIndexContent(Context context, DSpaceObject dso) throws SQLException, IOException {

    }

    @Override
    public void createIndex(Context context) throws SQLException, IOException {

    }

    @Override
    public void updateIndex(Context context) {

    }

    @Override
    public void updateIndex(Context context, boolean force) {
        try {
            int index = 0;
            ItemIterator items = null;
            try {
                for (items = Item.findAll(context); items.hasNext();) {
                    Item item = items.next();
                    System.out.println("Indexing item: " + index + " itemId: " + item.getID());
                    index++;
                    indexContent(context, item, force);
                    item.decache();
                }
            } finally {
                if (items != null)
                {
                    items.close();
                }
            }

            Collection[] collections = Collection.findAll(context);
            for (Collection collection : collections) {
                indexContent(context, collection, force);
                context.removeCached(collection, collection.getID());

            }

            Community[] communities = Community.findAll(context);
            for (Community community : communities) {
                indexContent(context, community, force);
                context.removeCached(community, community.getID());
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            e.printStackTrace();
        }

    }

    @Override
    public void cleanIndex(boolean force) throws IOException, SQLException, SearchServiceException {

    }

    @Override
    public void optimize() {

    }

    protected Client getIndexClient(){
        if(indexClient == null){
            ImmutableSettings.Builder settings = ImmutableSettings.settingsBuilder();
            NodeBuilder nBuilder = NodeBuilder.nodeBuilder().settings(settings);
            Node node = nBuilder.build().start();
            indexClient = node.client();
//            indexClient.admin().indices().create(new CreateIndexRequest(INDEX_NAME)).actionGet();
        }
        return indexClient;
    }
}
