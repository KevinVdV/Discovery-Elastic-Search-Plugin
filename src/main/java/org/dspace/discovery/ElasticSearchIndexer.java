package org.dspace.discovery;

import org.apache.log4j.Logger;
import org.dspace.content.*;
import org.dspace.core.Constants;
import org.dspace.core.Context;
import org.dspace.handle.HandleManager;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.action.index.IndexRequestBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.io.IOException;
import java.sql.SQLException;

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

    private void buildDocument(Context context, Item item, IndexRequestBuilder repository) throws IOException {
        DCValue[] metadata = item.getMetadata(Item.ANY, Item.ANY, Item.ANY, Item.ANY);

        XContentBuilder itemBuilder = XContentFactory.jsonBuilder().startObject();
        for (DCValue metadataValue : metadata) {
            String fieldName = metadataValue.schema + "." + metadataValue.element;
            if (metadataValue.qualifier != null) {
                fieldName += metadataValue.qualifier;
            }

            itemBuilder.field(fieldName, metadataValue.value);
        }
        byte[] source = itemBuilder.endObject().copiedBytes();
        repository.setSource(source);
        repository.execute().actionGet();

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
