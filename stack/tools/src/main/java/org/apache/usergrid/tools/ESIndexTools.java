/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.usergrid.tools;


import com.sun.jersey.api.spring.Autowire;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.usergrid.management.ManagementService;
import org.apache.usergrid.persistence.Entity;
import org.apache.usergrid.persistence.EntityManager;
import org.apache.usergrid.persistence.EntityManagerFactory;
import org.apache.usergrid.persistence.Results;
import org.apache.usergrid.persistence.cassandra.CassandraService;
import org.apache.usergrid.persistence.entities.Application;
import org.apache.usergrid.persistence.exceptions.DuplicateUniquePropertyExistsException;
import org.apache.usergrid.persistence.index.query.Query;
import org.apache.usergrid.utils.UUIDUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;
import java.util.Map.Entry;


/**
 * This is a utility to load all entities in an application and re-save them, this forces the secondary indexing to be
 * updated.
 *
 * @author tnine
 */
public class ESIndexTools extends ToolBase {

    /**
     *
     */
    private static final String APPLICATION_ARG = "app";

    /**
     *
     */
    private static final String COLLECTION_ARG = "col";

    /**
     *
     */
    private static final int PAGE_SIZE = 100;


    private static final Logger logger = LoggerFactory.getLogger( ESIndexTools.class );

    @Autowired
    private ManagementService ms;

    @Autowired
    private EntityManagerFactory emf;




    @Override
    @SuppressWarnings("static-access")
    public Options createOptions() {

        Option hostOption =
                OptionBuilder.withArgName( "host" ).hasArg().isRequired( true ).withDescription( "Cassandra host" )
                             .create( "host" );

        Option appOption = OptionBuilder.withArgName( APPLICATION_ARG ).hasArg().isRequired( false )
                                        .withDescription( "application id or app name" ).create( APPLICATION_ARG );

        Option collectionOption = OptionBuilder.withArgName( COLLECTION_ARG ).hasArg().isRequired( false )
                                               .withDescription( "colleciton name" ).create( COLLECTION_ARG );


        Options options = new Options();
        options.addOption( hostOption );
        options.addOption( appOption );
        options.addOption( collectionOption );

        return options;
    }


    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.usergrid.tools.ToolBase#runTool(org.apache.commons.cli.CommandLine)
     */
    @Override
    public void runTool( CommandLine line ) throws Exception {
        startSpring();

        logger.info( "Starting index rebuild" );

        /**
         * Goes through each app id specified
         */
        for ( UUID appId : getAppIds( line ) ) {

            logger.info( "Reindexing for app id: {}", appId );

            Set<String> collections = getCollections( line, appId );

            for ( String collection : collections ) {

                reindex( appId, collection );
            }
        }

        logger.info( "Finished index rebuild" );
    }


    /** Get all app id */
    private Collection<UUID> getAppIds( CommandLine line ) throws Exception {
        String appId = line.getOptionValue( APPLICATION_ARG );

        if ( appId != null ) {

            UUID id = UUIDUtils.tryExtractUUID( appId );

            if ( id == null ) {
                id = emf.getApplications().get( appId );
            }

            return Collections.singleton( id );
        }

        Map<String, UUID> ids = emf.getApplications();

        System.out.println( "Printing all apps" );

        for ( Entry<String, UUID> entry : ids.entrySet() ) {
            System.out.println( entry.getKey() );
        }

        return ids.values();
    }


    /** Get collection names. If none are specified, all are returned */
    private Set<String> getCollections( CommandLine line, UUID appId ) throws Exception {

        String passedName = line.getOptionValue( COLLECTION_ARG );

        if ( passedName != null ) {
            return Collections.singleton( passedName );
        }

        EntityManager em = emf.getEntityManager( appId );

        return em.getApplicationCollections();
    }


    /** The application id. The collection name. */
    private void reindex( UUID appId, String collectionName ) throws Exception {

        //Get graph API for this app.
        //where appId is the source, get collection

        //re-index each entity in the collection, and traverse it's connections
        CassandraService.MANAGEMENT_APPLICATION

        logger.info( "Reindexing collection: {} for app id: {}", collectionName, appId );

        //get management service


        EntityManager em = emf.getEntityManager( appId );
        Application app = em.getApplication();

        // search for all orgs

        Query query = new Query();
        query.setLimit( PAGE_SIZE );
        Results r = null;

        do {

            r = em.searchCollection( app, collectionName, query );

            for ( Entity entity : r.getEntities() ) {
                logger.info( "Updating entity type: {} with id: {} for app id: {}", new Object[] {
                        entity.getType(), entity.getUuid(), appId
                } );

                try {
                    em.update( entity );
                }
                catch ( DuplicateUniquePropertyExistsException dupee ) {
                    logger.error( "duplicate property for type: {} with id: {} for app id: {}.  Property name: {} , "
                            + "value: {}", new Object[] {
                            entity.getType(), entity.getUuid(), appId, dupee.getPropertyName(), dupee.getPropertyValue()
                    } );
                }
            }

            query.setCursor( r.getCursor() );
        }
        while ( r != null && r.size() == PAGE_SIZE );
    }
}
