/*******************************************************************************
 * Copyright 2012 Apigee Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.apache.usergrid.rest.applications.users;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.ws.rs.core.MediaType;

import org.apache.usergrid.cassandra.Concurrent;
import org.apache.usergrid.management.ApplicationInfo;
import org.apache.usergrid.management.OrganizationInfo;
import org.apache.usergrid.rest.AbstractRestIT;
import org.apache.usergrid.rest.applications.utils.UserRepo;
import org.apache.usergrid.utils.UUIDUtils;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.usergrid.java.client.Client.Query;
import org.apache.usergrid.java.client.entities.Activity;
import org.apache.usergrid.java.client.entities.Activity.ActivityObject;
import org.apache.usergrid.java.client.entities.Entity;
import org.apache.usergrid.java.client.entities.User;
import org.apache.usergrid.java.client.response.ApiResponse;

import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.UniformInterfaceException;

import static org.apache.usergrid.rest.applications.utils.TestUtils.getIdFromSearchResults;
import static org.apache.usergrid.utils.MapUtils.hashMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * @author zznate
 * @author tnine
 */
@Concurrent()
public class UserResourceIT extends AbstractRestIT {

    private static Logger log = LoggerFactory.getLogger( UserResourceIT.class );


    @Test
    public void usernameQuery() {

        UserRepo.INSTANCE.load( resource(), access_token );

        String ql = "username = 'user*'";

        JsonNode node = resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users" ).queryParam( "ql", ql )
                .queryParam( "access_token", access_token ).accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON_TYPE ).get( JsonNode.class );

        assertEquals( UserRepo.INSTANCE.getByUserName( "user1" ), getIdFromSearchResults( node, 0 ) );
        assertEquals( UserRepo.INSTANCE.getByUserName( "user2" ), getIdFromSearchResults( node, 1 ) );
        assertEquals( UserRepo.INSTANCE.getByUserName( "user3" ), getIdFromSearchResults( node, 2 ) );
    }


    @Test
    public void nameQuery() {

        UserRepo.INSTANCE.load( resource(), access_token );

        String ql = "name = 'John*'";

        JsonNode node = resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users" ).queryParam( "ql", ql )
                .queryParam( "access_token", access_token ).accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON_TYPE ).get( JsonNode.class );

        assertEquals( UserRepo.INSTANCE.getByUserName( "user2" ), getIdFromSearchResults( node, 0 ) );
        assertEquals( UserRepo.INSTANCE.getByUserName( "user3" ), getIdFromSearchResults( node, 1 ) );
    }


    @Test
    public void nameQueryByUUIDs() throws Exception {
        UserRepo.INSTANCE.load( resource(), access_token );

        String ql = "select uuid name = 'John*'";

        ApplicationInfo appInfo = setup.getMgmtSvc().getApplicationInfo( "org.apache.usergrid.test-organization/org.apache.usergrid.test-app" );
        OrganizationInfo orgInfo = setup.getMgmtSvc().getOrganizationByName( "org.apache.usergrid.test-organization" );

        resource().path( "/" + orgInfo.getUuid() + "/" + appInfo.getId() + "/users" ).queryParam( "ql", ql )
                .queryParam( "access_token", access_token ).accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON_TYPE ).get( JsonNode.class );
    }


    @Test
    public void nameFullTextQuery() {

        UserRepo.INSTANCE.load( resource(), access_token );

        String ql = "name contains 'Smith' order by name ";

        JsonNode node = resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users" ).queryParam( "ql", ql )
                .queryParam( "access_token", access_token ).accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON_TYPE ).get( JsonNode.class );

        assertEquals( UserRepo.INSTANCE.getByUserName( "user1" ), getIdFromSearchResults( node, 0 ) );
        assertEquals( UserRepo.INSTANCE.getByUserName( "user2" ), getIdFromSearchResults( node, 1 ) );
        assertEquals( UserRepo.INSTANCE.getByUserName( "user3" ), getIdFromSearchResults( node, 2 ) );
    }


    /** Tests that when a full text index is run on a field that isn't full text indexed an error is thrown */
    @Test(expected = UniformInterfaceException.class)
    public void fullTextQueryNotFullTextIndexed() {

        UserRepo.INSTANCE.load( resource(), access_token );

        String ql = "username contains 'user' ";

        resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users" ).queryParam( "ql", ql )
                .queryParam( "access_token", access_token ).accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON_TYPE ).get( JsonNode.class );
    }


    /** Tests that when a full text index is run on a field that isn't full text indexed an error is thrown */
    @Test(expected = UniformInterfaceException.class)
    public void fullQueryNotIndexed() {

        UserRepo.INSTANCE.load( resource(), access_token );

        String ql = "picture = 'foo' ";

        resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users" ).queryParam( "ql", ql )
                .queryParam( "access_token", access_token ).accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON_TYPE ).get( JsonNode.class );
    }


    /** Test that when activity is pushed with not actor, it's set to the user who created it */
    @Test
    public void emtpyActorActivity() {
        UserRepo.INSTANCE.load( resource(), access_token );
        UUID userId = UserRepo.INSTANCE.getByUserName( "user1" );

        Activity activity = new Activity();
        activity.setProperty( "email", "rod@rodsimpson.com" );
        activity.setProperty( "verb", "POST" );
        activity.setProperty( "content", "Look! more new content" );

        ApiResponse response = client.postUserActivity( userId.toString(), activity );

        assertNull( "Error was: " + response.getErrorDescription(), response.getError() );

        Entity entity = response.getEntities().get( 0 );

        UUID activityId = entity.getUuid();

        assertNotNull( activityId );

        JsonNode actor = getActor( entity );

        UUID actorId = UUIDUtils.tryGetUUID( actor.get( "uuid" ).getTextValue() );

        assertEquals( userId, actorId );

        assertEquals( "user1@apigee.com", actor.get( "email" ).asText() );
    }


    /** Insert the uuid and email if they're empty in the request */
    @Test
    public void noUUIDorEmail() {

        UserRepo.INSTANCE.load( resource(), access_token );
        UUID userId = UserRepo.INSTANCE.getByUserName( "user1" );

        Activity activity = new Activity();
        activity.setProperty( "email", "rod@rodsimpson.com" );
        activity.setProperty( "verb", "POST" );
        activity.setProperty( "content", "Look! more new content" );

        // same as above, but with actor partially filled out

        ActivityObject actorPost = new ActivityObject();
        actorPost.setDisplayName( "Dino" );

        activity.setActor( actorPost );

        ApiResponse response = client.postUserActivity( userId.toString(), activity );

        assertNull( "Error was: " + response.getErrorDescription(), response.getError() );

        Entity entity = response.getEntities().get( 0 );

        UUID activityId = entity.getUuid();

        assertNotNull( activityId );

        JsonNode actor = getActor( entity );

        UUID actorId = UUIDUtils.tryGetUUID( actor.get( "uuid" ).getTextValue() );

        assertEquals( userId, actorId );

        assertEquals( "user1@apigee.com", actor.get( "email" ).asText() );
    }


    /** Don't touch the UUID when it's already set in the JSON */
    @Test
    public void ignoreUUIDandEmail() {
        UserRepo.INSTANCE.load( resource(), access_token );
        UUID userId = UserRepo.INSTANCE.getByUserName( "user1" );

        UUID testUUID = UUIDUtils.newTimeUUID();
        String testEmail = "foo@bar.com";

        // same as above, but with actor partially filled out
        Activity activity = new Activity();
        activity.setProperty( "email", "rod@rodsimpson.com" );
        activity.setProperty( "verb", "POST" );
        activity.setProperty( "content", "Look! more new content" );

        // same as above, but with actor partially filled out

        ActivityObject actorPost = new ActivityObject();
        actorPost.setDisplayName( "Dino" );
        actorPost.setUuid( testUUID );
        actorPost.setDynamicProperty( "email", testEmail );

        activity.setActor( actorPost );

        ApiResponse response = client.postUserActivity( userId.toString(), activity );

        assertNull( "Error was: " + response.getErrorDescription(), response.getError() );

        Entity entity = response.getEntities().get( 0 );

        UUID activityId = entity.getUuid();

        assertNotNull( activityId );

        JsonNode actor = getActor( entity );

        UUID actorId = UUIDUtils.tryGetUUID( actor.get( "uuid" ).getTextValue() );

        assertEquals( testUUID, actorId );

        assertEquals( testEmail, actor.get( "email" ).asText() );
    }


    /** Test that when activity is pushed with not actor, it's set to the user who created it */
    @Test
    public void userActivitiesDefaultOrder() {
        UserRepo.INSTANCE.load( resource(), access_token );
        UUID userId = UserRepo.INSTANCE.getByUserName( "user1" );

        Activity activity = new Activity();
        activity.setProperty( "email", "rod@rodsimpson.com" );
        activity.setProperty( "verb", "POST" );
        activity.setProperty( "content", "activity 1" );

        ApiResponse response = client.postUserActivity( userId.toString(), activity );

        assertNull( "Error was: " + response.getErrorDescription(), response.getError() );

        Entity entity = response.getFirstEntity();

        UUID firstActivityId = entity.getUuid();

        activity = new Activity();
        activity.setProperty( "email", "rod@rodsimpson.com" );
        activity.setProperty( "verb", "POST" );
        activity.setProperty( "content", "activity 2" );

        response = client.postUserActivity( userId.toString(), activity );

        assertNull( "Error was: " + response.getErrorDescription(), response.getError() );

        entity = response.getFirstEntity();

        UUID secondActivityId = entity.getUuid();

        Query query = client.queryActivity();

        entity = query.getResponse().getEntities().get( 0 );

        assertEquals( secondActivityId, entity.getUuid() );

        entity = query.getResponse().getEntities().get( 1 );

        assertEquals( firstActivityId, entity.getUuid() );
    }


    @Test
    public void getUserWIthEmailUsername() {
        UUID id = UUIDUtils.newTimeUUID();

        String username = "username-email" + "@usergrid.org";
        String name = "name" + id;
        String email = "email" + id + "@usergrid.org";

        ApiResponse response = client.createUser( username, name, email, "password" );

        assertNull( "Error was: " + response.getErrorDescription(), response.getError() );

        Entity userEntity = response.getEntities().get( 0 );

        // get the user with username property that has an email value
        JsonNode node = resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/" + username )
                .queryParam( "access_token", access_token ).accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON_TYPE ).get( JsonNode.class );

        assertEquals( username, node.get( "entities" ).get( 0 ).get( "username" ).asText() );
        assertEquals( name, node.get( "entities" ).get( 0 ).get( "name" ).asText() );
        assertEquals( email, node.get( "entities" ).get( 0 ).get( "email" ).asText() );

        // get the user with email property value
        node = resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/" + email )
                .queryParam( "access_token", access_token ).accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON_TYPE ).get( JsonNode.class );

        assertEquals( username, node.get( "entities" ).get( 0 ).get( "username" ).asText() );
        assertEquals( name, node.get( "entities" ).get( 0 ).get( "name" ).asText() );
        assertEquals( email, node.get( "entities" ).get( 0 ).get( "email" ).asText() );
    }


    /** Tests that when querying all users, we get the same result size when using "order by" */
    @Test
    public void resultSizeSame() {
        UserRepo.INSTANCE.load( resource(), access_token );
        UUID userId1 = UserRepo.INSTANCE.getByUserName( "user1" );
        UUID userId2 = UserRepo.INSTANCE.getByUserName( "user2" );
        UUID userId3 = UserRepo.INSTANCE.getByUserName( "user3" );

        Query query = client.queryUsers();

        ApiResponse response = query.getResponse();

        assertNull( "Error was: " + response.getErrorDescription(), response.getError() );

        int nonOrderedSize = response.getEntities().size();

        query = client.queryUsers( "order by username" );

        response = query.getResponse();

        int orderedSize = response.getEntities().size();

        assertEquals( "Sizes match", nonOrderedSize, orderedSize );

        int firstEntityIndex = getEntityIndex( userId1, response );

        int secondEntityIndex = getEntityIndex( userId2, response );

        int thirdEntityIndex = getEntityIndex( userId3, response );

        assertTrue( "Ordered correctly", firstEntityIndex < secondEntityIndex );

        assertTrue( "Ordered correctly", secondEntityIndex < thirdEntityIndex );
    }


    private int getEntityIndex( UUID entityId, ApiResponse response ) {
        List<Entity> entities = response.getEntities();

        for ( int i = 0; i < entities.size(); i++ ) {
            if ( entityId.equals( entities.get( i ).getUuid() ) ) {
                return i;
            }
        }

        return -1;
    }


    @Test
    public void clientNameQuery() {

        UUID id = UUIDUtils.newTimeUUID();

        String username = "username" + id;
        String name = "name" + id;

        ApiResponse response = client.createUser( username, name, id + "@usergrid.org", "password" );

        assertNull( "Error was: " + response.getErrorDescription(), response.getError() );

        UUID createdId = response.getEntities().get( 0 ).getUuid();

        Query results = client.queryUsers( String.format( "name = '%s'", name ) );
        User user = results.getResponse().getEntities( User.class ).get( 0 );

        assertEquals( createdId, user.getUuid() );
    }


    @Test
    public void deleteUser() {

        UUID id = UUIDUtils.newTimeUUID();

        String username = "username" + id;
        String name = "name" + id;

        ApiResponse response = client.createUser( username, name, id + "@usergrid.org", "password" );

        assertNull( "Error was: " + response.getErrorDescription(), response.getError() );

        UUID createdId = response.getEntities().get( 0 ).getUuid();

        JsonNode node = resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/" + createdId )
                .queryParam( "access_token", access_token ).accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON_TYPE ).delete( JsonNode.class );

        assertNull( node.get( "errors" ) );

        Query results = client.queryUsers( String.format( "username = '%s'", name ) );
        assertEquals( 0, results.getResponse().getEntities( User.class ).size() );

        // now create that same user again, it should work
        response = client.createUser( username, name, id + "@usergrid.org", "password" );

        assertNull( "Error was: " + response.getErrorDescription(), response.getError() );

        createdId = response.getEntities().get( 0 ).getUuid();

        assertNotNull( createdId );
    }


    @Test
    public void singularCollectionName() {
        UUID id = UUIDUtils.newTimeUUID();

        String username = "username1" + id;
        String name = "name1" + id;
        String email = "email1" + id + "@usergrid.org";

        ApiResponse response = client.createUser( username, name, email, "password" );

        assertNull( "Error was: " + response.getErrorDescription(), response.getError() );

        UUID firstCreatedId = response.getEntities().get( 0 ).getUuid();

        username = "username2" + id;
        name = "name2" + id;
        email = "email2" + id + "@usergrid.org";

        response = client.createUser( username, name, email, "password" );

        assertNull( "Error was: " + response.getErrorDescription(), response.getError() );

        UUID secondCreatedId = response.getEntities().get( 0 ).getUuid();

        // now create a connection of "likes" between the first user and the
        // second using pluralized form

        // plural collection name
        String path = String.format( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/%s/conn1/%s", firstCreatedId, secondCreatedId );

        JsonNode node =
                resource().path( path ).queryParam( "access_token", access_token ).accept( MediaType.APPLICATION_JSON )
                        .type( MediaType.APPLICATION_JSON_TYPE ).post( JsonNode.class );

        assertEquals( secondCreatedId.toString(), getEntity( node, 0 ).get( "uuid" ).asText() );

        // singular collection name
        path = String.format( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/user/%s/conn2/%s", firstCreatedId, secondCreatedId );

        node = resource().path( path ).queryParam( "access_token", access_token ).accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON_TYPE ).post( JsonNode.class );

        assertEquals( secondCreatedId.toString(), getEntity( node, 0 ).get( "uuid" ).asText() );

        path = String.format( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/%s/conn1", firstCreatedId );

        node = resource().path( path ).queryParam( "access_token", access_token ).accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON_TYPE ).get( JsonNode.class );

        assertEquals( secondCreatedId.toString(), getEntity( node, 0 ).get( "uuid" ).asText() );

        path = String.format( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/user/%s/conn1", firstCreatedId );

        node = resource().path( path ).queryParam( "access_token", access_token ).accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON_TYPE ).get( JsonNode.class );

        assertEquals( secondCreatedId.toString(), getEntity( node, 0 ).get( "uuid" ).asText() );

        path = String.format( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/%s/conn2", firstCreatedId );

        node = resource().path( path ).queryParam( "access_token", access_token ).accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON_TYPE ).get( JsonNode.class );

        assertEquals( secondCreatedId.toString(), getEntity( node, 0 ).get( "uuid" ).asText() );

        path = String.format( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/user/%s/conn2", firstCreatedId );

        node = resource().path( path ).queryParam( "access_token", access_token ).accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON_TYPE ).get( JsonNode.class );

        assertEquals( secondCreatedId.toString(), getEntity( node, 0 ).get( "uuid" ).asText() );
    }


    @Test
    public void connectionByNameAndType() {
        UUID id = UUIDUtils.newTimeUUID();

        String username1 = "username1" + id;
        String name1 = "name1" + id;
        String email1 = "email1" + id + "@usergrid.org";

        ApiResponse response = client.createUser( username1, name1, email1, "password" );

        assertNull( "Error was: " + response.getErrorDescription(), response.getError() );

        UUID firstCreatedId = response.getEntities().get( 0 ).getUuid();

        String username2 = "username2" + id;
        String name2 = "name2" + id;
        String email2 = "email2" + id + "@usergrid.org";

        response = client.createUser( username2, name2, email2, "password" );

        assertNull( "Error was: " + response.getErrorDescription(), response.getError() );

        UUID secondCreatedId = response.getEntities().get( 0 ).getUuid();

        // now create a connection of "likes" between the first user and the
        // second using pluralized form

        // named entity in collection name
        String path = String.format( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/%s/conn1/users/%s", firstCreatedId, username2 );

        JsonNode node =
                resource().path( path ).queryParam( "access_token", access_token ).accept( MediaType.APPLICATION_JSON )
                        .type( MediaType.APPLICATION_JSON_TYPE ).post( JsonNode.class );

        assertEquals( secondCreatedId.toString(), getEntity( node, 0 ).get( "uuid" ).asText() );

        // named entity in collection name
        path = String.format( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/%s/conn2/users/%s", username1, username2 );

        node = resource().path( path ).queryParam( "access_token", access_token ).accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON_TYPE ).post( JsonNode.class );

        assertEquals( secondCreatedId.toString(), getEntity( node, 0 ).get( "uuid" ).asText() );
    }


    /** Usergrid-1222 org.apache.usergrid.test */
    @Test
    public void connectionQuerybyEmail() {
        UUID id = UUIDUtils.newTimeUUID();

        String name = "name1" + id;
        String email = "email1" + id + "@usergrid.org";

        ApiResponse response = client.createUser( email, name, email, "password" );

        assertNull( "Error was: " + response.getErrorDescription(), response.getError() );

        UUID userId = response.getEntities().get( 0 ).getUuid();

        Entity role = new Entity( "role" );
        role.setProperty( "name", "connectionQuerybyEmail1" );

        response = client.createEntity( role );

        assertNull( "Error was: " + response.getErrorDescription(), response.getError() );

        UUID roleId1 = response.getEntities().get( 0 ).getUuid();

        //add permissions to the role

        Map<String, String> perms = new HashMap<String, String>();
        perms.put( "permission", "get:/stuff/**" );

        String path = String.format( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/roles/%s/permissions", roleId1 );

        JsonNode node =
                resource().path( path ).queryParam( "access_token", access_token ).accept( MediaType.APPLICATION_JSON )
                        .type( MediaType.APPLICATION_JSON_TYPE ).post( JsonNode.class, perms );


        //Create the second role
        role = new Entity( "role" );
        role.setProperty( "name", "connectionQuerybyEmail2" );

        response = client.createEntity( role );

        assertNull( "Error was: " + response.getErrorDescription(), response.getError() );

        UUID roleId2 = response.getEntities().get( 0 ).getUuid();

        //add permissions to the role

        perms = new HashMap<String, String>();
        perms.put( "permission", "get:/stuff/**" );

        path = String.format( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/roles/%s/permissions", roleId2 );

        node = resource().path( path ).queryParam( "access_token", access_token ).accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON_TYPE ).post( JsonNode.class, perms );


        //connect the entities where role is the root
        path = String.format( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/roles/%s/users/%s", roleId1, userId );

        node = resource().path( path ).queryParam( "access_token", access_token ).accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON_TYPE ).post( JsonNode.class );

        // now create a connection of "likes" between the first user and the
        // second using pluralized form

        assertEquals( userId.toString(), getEntity( node, 0 ).get( "uuid" ).asText() );


        //connect the second role
        path = String.format( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/roles/%s/users/%s", roleId2, userId );

        node = resource().path( path ).queryParam( "access_token", access_token ).accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON_TYPE ).post( JsonNode.class );


        assertEquals( userId.toString(), getEntity( node, 0 ).get( "uuid" ).asText() );

        //query the second role, it should work
        path = String.format( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/roles/%s/users", roleId2 );

        node = resource().path( path ).queryParam( "access_token", access_token )
                .queryParam( "ql", "select%20*%20where%20username%20=%20'" + email + "'" )
                .accept( MediaType.APPLICATION_JSON ).type( MediaType.APPLICATION_JSON_TYPE ).get( JsonNode.class );

        assertEquals( userId.toString(), getEntity( node, 0 ).get( "uuid" ).asText() );


        //query the first role, it should work
        path = String.format( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/roles/%s/users", roleId1 );

        node = resource().path( path ).queryParam( "access_token", access_token )
                .queryParam( "ql", "select%20*%20where%20username%20=%20'" + email + "'" )
                .accept( MediaType.APPLICATION_JSON ).type( MediaType.APPLICATION_JSON_TYPE ).get( JsonNode.class );

        assertEquals( userId.toString(), getEntity( node, 0 ).get( "uuid" ).asText() );


        //now delete the first role
        path = String.format( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/roles/%s", roleId1 );

        node = resource().path( path ).queryParam( "access_token", access_token ).accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON_TYPE ).delete( JsonNode.class );

        //query the first role, it should 404
        path = String.format( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/roles/%s/users", roleId1 );

        try {
            node = resource().path( path ).queryParam( "access_token", access_token )
                    .queryParam( "ql", "select%20*%20where%20username%20=%20'" + email + "'" )
                    .accept( MediaType.APPLICATION_JSON ).type( MediaType.APPLICATION_JSON_TYPE ).get( JsonNode.class );
        }
        catch ( UniformInterfaceException e ) {
            assertEquals( Status.NOT_FOUND, e.getResponse().getClientResponseStatus() );
        }

        //query the second role, it should work
        path = String.format( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/roles/%s/users", roleId2 );

        node = resource().path( path ).queryParam( "access_token", access_token )
                .queryParam( "ql", "select%20*%20where%20username%20=%20'" + email + "'" )
                .accept( MediaType.APPLICATION_JSON ).type( MediaType.APPLICATION_JSON_TYPE ).get( JsonNode.class );

        assertEquals( userId.toString(), getEntity( node, 0 ).get( "uuid" ).asText() );
    }


    @Test
    public void connectionByNameAndDynamicType() {
        UUID id = UUIDUtils.newTimeUUID();

        String username1 = "username1" + id;
        String name1 = "name1" + id;
        String email1 = "email1" + id + "@usergrid.org";

        ApiResponse response = client.createUser( username1, name1, email1, "password" );

        assertNull( "Error was: " + response.getErrorDescription(), response.getError() );

        UUID firstCreatedId = response.getEntities().get( 0 ).getUuid();

        String name = "pepperoni";

        Entity pizza = new Entity();
        pizza.setProperty( "name", name );
        pizza.setType( "pizza" );

        response = client.createEntity( pizza );

        assertNull( "Error was: " + response.getErrorDescription(), response.getError() );

        UUID secondCreatedId = response.getEntities().get( 0 ).getUuid();

        // now create a connection of "likes" between the first user and the
        // second using pluralized form

        // named entity in collection name
        String path = String.format( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/%s/conn1/pizzas/%s", firstCreatedId,
                secondCreatedId );

        JsonNode node =
                resource().path( path ).queryParam( "access_token", access_token ).accept( MediaType.APPLICATION_JSON )
                        .type( MediaType.APPLICATION_JSON_TYPE ).post( JsonNode.class );

        assertEquals( secondCreatedId.toString(), getEntity( node, 0 ).get( "uuid" ).asText() );

        // named entity in collection name
        path = String.format( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/%s/conn2/pizzas/%s", username1, name );

        node = resource().path( path ).queryParam( "access_token", access_token ).accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON_TYPE ).post( JsonNode.class );

        assertEquals( secondCreatedId.toString(), getEntity( node, 0 ).get( "uuid" ).asText() );
    }


    @Test
    public void nameUpdate() {
        UUID id = UUIDUtils.newTimeUUID();

        String username = "username" + id;
        String name = "name" + id;
        String email = "email" + id + "@usergrid.org";

        ApiResponse response = client.createUser( username, name, email, "password" );

        assertNull( "Error was: " + response.getErrorDescription(), response.getError() );

        Entity userEntity = response.getEntities().get( 0 );

        // attempt to log in
        JsonNode node = resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/token" ).queryParam( "username", username )
                .queryParam( "password", "password" ).queryParam( "grant_type", "password" )
                .accept( MediaType.APPLICATION_JSON ).type( MediaType.APPLICATION_JSON_TYPE ).get( JsonNode.class );

        assertEquals( username, node.get( "user" ).get( "username" ).asText() );
        assertEquals( name, node.get( "user" ).get( "name" ).asText() );
        assertEquals( email, node.get( "user" ).get( "email" ).asText() );

        // now update the name and email
        String newName = "newName";
        String newEmail = "newEmail" + UUIDUtils.newTimeUUID() + "@usergrid.org";

        userEntity.setProperty( "name", newName );
        userEntity.setProperty( "email", newEmail );
        userEntity.setProperty( "password", "newp2ssword" );
        userEntity.setProperty( "pin", "newp1n" );

        node = resource().path( String.format( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/%s", username ) )
                .queryParam( "access_token", access_token ).accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON_TYPE ).put( JsonNode.class, userEntity.getProperties() );

        // now see if we've updated
        node = resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/token" ).queryParam( "username", username )
                .queryParam( "password", "password" ).queryParam( "grant_type", "password" )
                .accept( MediaType.APPLICATION_JSON ).type( MediaType.APPLICATION_JSON_TYPE ).get( JsonNode.class );

        assertEquals( username, node.get( "user" ).get( "username" ).asText() );
        assertEquals( newName, node.get( "user" ).get( "name" ).asText() );
        assertEquals( newEmail, node.get( "user" ).get( "email" ).asText() );
        assertNull( newEmail, node.get( "user" ).get( "password" ) );
        assertNull( newEmail, node.get( "user" ).get( "pin" ) );
    }


    /**
     *
     * @return
     */
    public JsonNode getActor( Entity entity ) {
        return entity.getProperties().get( "actor" );
    }


    @Test
    public void test_POST_batch() {

        log.info( "UserResourceIT.test_POST_batch" );

        JsonNode node = null;

        List<Map<String, Object>> batch = new ArrayList<Map<String, Object>>();

        Map<String, Object> properties = new LinkedHashMap<String, Object>();
        properties.put( "username", "test_user_1" );
        properties.put( "email", "user1@org.apache.usergrid.test.com" );
        batch.add( properties );

        properties = new LinkedHashMap<String, Object>();
        properties.put( "username", "test_user_2" );
        batch.add( properties );

        properties = new LinkedHashMap<String, Object>();
        properties.put( "username", "test_user_3" );
        batch.add( properties );

        node = resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/" ).queryParam( "access_token", access_token )
                .accept( MediaType.APPLICATION_JSON ).type( MediaType.APPLICATION_JSON_TYPE )
                .post( JsonNode.class, batch );

        assertNotNull( node );
        logNode( node );
    }


    @Test
    public void deactivateUser() {

        UUID newUserUuid = UUIDUtils.newTimeUUID();

        String userName = String.format( "org.apache.usergrid.test%s", newUserUuid );

        Map<String, String> payload =
                hashMap( "email", String.format( "%s@anuff.com", newUserUuid ) ).map( "username", userName )
                        .map( "name", "Ed Anuff" ).map( "password", "sesame" ).map( "pin", "1234" );

        resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users" ).queryParam( "access_token", access_token )
                .accept( MediaType.APPLICATION_JSON ).type( MediaType.APPLICATION_JSON_TYPE )
                .post( JsonNode.class, payload );

        JsonNode response =
                resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users" ).queryParam( "access_token", access_token )
                        .accept( MediaType.APPLICATION_JSON ).type( MediaType.APPLICATION_JSON_TYPE )
                        .get( JsonNode.class );

        // disable the user

        Map<String, String> data = new HashMap<String, String>();

        response = resource().path( String.format( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/%s/deactivate", userName ) )
                .queryParam( "access_token", access_token ).accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON_TYPE ).post( JsonNode.class, data );

        JsonNode entity = getEntity( response, 0 );

        assertFalse( entity.get( "activated" ).asBoolean() );
        assertNotNull( entity.get( "deactivated" ) );
    }


    @Test
    public void test_PUT_password_fail() {

        boolean fail = false;
        try {
            ApiResponse changeResponse = client.changePassword( "edanuff", "foo", "bar" );
            fail = changeResponse.getError() != null;
        }
        catch ( Exception e ) {
            fail = true;
        }
        assertTrue( fail );
    }


    @Test
    public void test_GET_user_ok() throws InterruptedException {

        // TODO figure out what is being overridden? why 400?
        JsonNode node =
                resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users" ).queryParam( "access_token", access_token )
                        .accept( MediaType.APPLICATION_JSON ).type( MediaType.APPLICATION_JSON_TYPE )
                        .get( JsonNode.class );

        String uuid = node.get( "entities" ).get( 0 ).get( "uuid" ).getTextValue();

        node = resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/" + uuid ).queryParam( "access_token", access_token )
                .accept( MediaType.APPLICATION_JSON ).type( MediaType.APPLICATION_JSON_TYPE ).get( JsonNode.class );
        logNode( node );
        assertEquals( "ed@anuff.com", node.get( "entities" ).get( 0 ).get( "email" ).getTextValue() );
    }


    @Test
    public void test_PUT_password_ok() {

        ApiResponse response = client.changePassword( "edanuff", "sesame", "sesame1" );

        assertNull( response.getError() );

        response = client.authorizeAppUser( "ed@anuff.com", "sesame1" );

        assertNull( response.getError() );

        // if this was successful, we need to re-set the password for other
        // tests
        response = client.changePassword( "edanuff", "sesame1", "sesame" );

        assertNull( response.getError() );
    }


    @Test
    public void setUserPasswordAsAdmin() {

        String newPassword = "foo";

        Map<String, String> data = new HashMap<String, String>();
        data.put( "newpassword", newPassword );

        // change the password as admin. The old password isn't required
        JsonNode node = resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/edanuff/password" )
                .queryParam( "access_token", adminAccessToken ).accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON_TYPE ).post( JsonNode.class, data );

        assertNull( getError( node ) );

        ApiResponse response = client.authorizeAppUser( "ed@anuff.com", newPassword );

        assertNull( response.getError() );
    }


    @Test
    public void passwordMismatchErrorUser() {
        String origPassword = "foo";
        String newPassword = "bar";

        Map<String, String> data = new HashMap<String, String>();
        data.put( "newpassword", origPassword );

        // now change the password, with an incorrect old password

        data.put( "oldpassword", origPassword );
        data.put( "newpassword", newPassword );

        Status responseStatus = null;
        try {
            resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/edanuff/password" ).accept( MediaType.APPLICATION_JSON )
                    .type( MediaType.APPLICATION_JSON_TYPE ).post( JsonNode.class, data );
        }
        catch ( UniformInterfaceException uie ) {
            responseStatus = uie.getResponse().getClientResponseStatus();
        }

        assertNotNull( responseStatus );

        assertEquals( Status.BAD_REQUEST, responseStatus );
    }


    @Test
    public void addRemoveRole() {

        UUID id = UUIDUtils.newTimeUUID();

        String roleName = "rolename" + id;

        String username = "username" + id;
        String name = "name" + id;
        String email = "email" + id + "@usergrid.org";

        ApiResponse response = client.createUser( username, name, email, "password" );
        assertNull( "Error was: " + response.getErrorDescription(), response.getError() );

        UUID createdId = response.getEntities().get( 0 ).getUuid();

        // create Role

        String json = "{\"title\":\"" + roleName + "\",\"name\":\"" + roleName + "\"}";
        JsonNode node =
                resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/roles" ).queryParam( "access_token", access_token )
                        .accept( MediaType.APPLICATION_JSON ).type( MediaType.APPLICATION_JSON_TYPE )
                        .post( JsonNode.class, json );

        // check it
        assertNull( node.get( "errors" ) );


        // add Role

        node = resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/" + createdId + "/roles/" + roleName )
                .queryParam( "access_token", access_token ).accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON_TYPE ).post( JsonNode.class );

        // check it
        assertNull( node.get( "errors" ) );
        assertEquals( node.get( "entities" ).get( 0 ).get( "name" ).asText(), roleName );

        node = resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/" + createdId + "/roles" )
                .queryParam( "access_token", access_token ).accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON_TYPE ).get( JsonNode.class );
        assertNull( node.get( "errors" ) );
        assertEquals( node.get( "entities" ).get( 0 ).get( "name" ).asText(), roleName );


        // remove Role

        node = resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/" + createdId + "/roles/" + roleName )
                .queryParam( "access_token", access_token ).accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON_TYPE ).delete( JsonNode.class );

        // check it
        assertNull( node.get( "errors" ) );

        node = resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/" + createdId + "/roles" )
                .queryParam( "access_token", access_token ).accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON_TYPE ).get( JsonNode.class );
        assertNull( node.get( "errors" ) );
        assertTrue( node.get( "entities" ).size() == 0 );
    }


    @Test
    public void revokeToken() throws Exception {

        String token1 = super.userToken( "edanuff", "sesame" );
        String token2 = super.userToken( "edanuff", "sesame" );

        JsonNode response =
                resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/edanuff" ).queryParam( "access_token", token1 )
                        .accept( MediaType.APPLICATION_JSON ).type( MediaType.APPLICATION_JSON_TYPE )
                        .get( JsonNode.class );

        assertNotNull( getEntity( response, 0 ) );

        response = resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/edanuff" ).queryParam( "access_token", token2 )
                .accept( MediaType.APPLICATION_JSON ).type( MediaType.APPLICATION_JSON_TYPE ).get( JsonNode.class );

        assertNotNull( getEntity( response, 0 ) );

        // now revoke the tokens
        response = resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/edanuff/revoketokens" )
                .queryParam( "access_token", adminAccessToken ).accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON_TYPE ).post( JsonNode.class );

        // the tokens shouldn't work

        Status status = null;

        try {
            response =
                    resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/edanuff" ).queryParam( "access_token", token1 )
                            .accept( MediaType.APPLICATION_JSON ).type( MediaType.APPLICATION_JSON_TYPE )
                            .get( JsonNode.class );
        }
        catch ( UniformInterfaceException uie ) {
            status = uie.getResponse().getClientResponseStatus();
        }

        assertEquals( Status.UNAUTHORIZED, status );

        status = null;

        try {
            response =
                    resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/edanuff" ).queryParam( "access_token", token2 )
                            .accept( MediaType.APPLICATION_JSON ).type( MediaType.APPLICATION_JSON_TYPE )
                            .get( JsonNode.class );
        }
        catch ( UniformInterfaceException uie ) {
            status = uie.getResponse().getClientResponseStatus();
        }

        assertEquals( Status.UNAUTHORIZED, status );

        String token3 = super.userToken( "edanuff", "sesame" );
        String token4 = super.userToken( "edanuff", "sesame" );

        response = resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/edanuff" ).queryParam( "access_token", token3 )
                .accept( MediaType.APPLICATION_JSON ).type( MediaType.APPLICATION_JSON_TYPE ).get( JsonNode.class );

        assertNotNull( getEntity( response, 0 ) );

        response = resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/edanuff" ).queryParam( "access_token", token4 )
                .accept( MediaType.APPLICATION_JSON ).type( MediaType.APPLICATION_JSON_TYPE ).get( JsonNode.class );

        assertNotNull( getEntity( response, 0 ) );

        // now revoke the token3
        response = resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/edanuff/revoketoken" )
                .queryParam( "access_token", token3 ).queryParam( "token", token3 ).accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON_TYPE ).post( JsonNode.class );

        // the token3 shouldn't work

        status = null;

        try {
            response =
                    resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/edanuff" ).queryParam( "access_token", token3 )
                            .accept( MediaType.APPLICATION_JSON ).type( MediaType.APPLICATION_JSON_TYPE )
                            .get( JsonNode.class );
        }
        catch ( UniformInterfaceException uie ) {
            status = uie.getResponse().getClientResponseStatus();
        }

        assertEquals( Status.UNAUTHORIZED, status );

        status = null;

        try {
            response =
                    resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/edanuff" ).queryParam( "access_token", token4 )
                            .accept( MediaType.APPLICATION_JSON ).type( MediaType.APPLICATION_JSON_TYPE )
                            .get( JsonNode.class );

            status = Status.OK;
        }
        catch ( UniformInterfaceException uie ) {
            status = uie.getResponse().getClientResponseStatus();
        }

        assertEquals( Status.OK, status );
    }


    @Test
    public void getToken() throws Exception {

        createUser( "test_1", "test_1@org.apache.usergrid.test.com", "test123", "Test1 User" ); // client.setApiUrl(apiUrl);
        createUser( "test_2", "test_2@org.apache.usergrid.test.com", "test123", "Test2 User" ); // client.setApiUrl(apiUrl);
        createUser( "test_3", "test_3@org.apache.usergrid.test.com", "test123", "Test3 User" ); // client.setApiUrl(apiUrl);

        ApplicationInfo appInfo = setup.getMgmtSvc().getApplicationInfo( "org.apache.usergrid.test-organization/org.apache.usergrid.test-app" );

        String clientId = setup.getMgmtSvc().getClientIdForApplication( appInfo.getId() );
        String clientSecret = setup.getMgmtSvc().getClientSecretForApplication( appInfo.getId() );

        JsonNode node =
                resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/test_1/token" ).queryParam( "client_id", clientId )
                        .queryParam( "client_secret", clientSecret ).accept( MediaType.APPLICATION_JSON )
                        .type( MediaType.APPLICATION_JSON_TYPE ).get( JsonNode.class );

        String user_token_from_client_credentials = node.get( "access_token" ).asText();

        UUID userId = UUID.fromString( node.get( "user" ).get( "uuid" ).asText() );
        setup.getMgmtSvc().activateAppUser( appInfo.getId(), userId );

        String user_token_from_java = setup.getMgmtSvc().getAccessTokenForAppUser( appInfo.getId(), userId, 1000000 );

        assertNotNull( user_token_from_client_credentials );

        Status status = null;

        // bad access token
        try {
            resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/test_1/token" ).queryParam( "access_token", "blah" )
                    .accept( MediaType.APPLICATION_JSON ).type( MediaType.APPLICATION_JSON_TYPE ).get( JsonNode.class );
        }
        catch ( UniformInterfaceException uie ) {
            status = uie.getResponse().getClientResponseStatus();
            log.info( "Error Response Body: " + uie.getResponse().getEntity( String.class ) );
        }

        assertEquals( Status.UNAUTHORIZED, status );

        try {
            resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/test_2/token" )
                    .queryParam( "access_token", user_token_from_client_credentials )
                    .accept( MediaType.APPLICATION_JSON ).type( MediaType.APPLICATION_JSON_TYPE ).get( JsonNode.class );
        }
        catch ( UniformInterfaceException uie ) {
            status = uie.getResponse().getClientResponseStatus();
            log.info( "Error Response Body: " + uie.getResponse().getEntity( String.class ) );
        }

        assertEquals( Status.FORBIDDEN, status );


        JsonNode response = null;
        response = resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/test_1" )
                .queryParam( "access_token", user_token_from_client_credentials ).accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON_TYPE ).get( JsonNode.class );

        assertNotNull( getEntity( response, 0 ) );

        response = resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/test_1" )
                .queryParam( "access_token", user_token_from_java ).accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON_TYPE ).get( JsonNode.class );

        assertNotNull( getEntity( response, 0 ) );

        setup.getMgmtSvc().deactivateUser( appInfo.getId(), userId );
        try {
            resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/token" ).queryParam( "grant_type", "password" )
                    .queryParam( "username", "test_1" ).queryParam( "password", "test123" )
                    .accept( MediaType.APPLICATION_JSON ).type( MediaType.APPLICATION_JSON_TYPE ).get( JsonNode.class );
            fail( "request for deactivated user should fail" );
        }
        catch ( UniformInterfaceException uie ) {
            status = uie.getResponse().getClientResponseStatus();
            JsonNode body = uie.getResponse().getEntity( JsonNode.class );
            assertEquals( "user not activated", body.findPath( "error_description" ).getTextValue() );
        }
    }


    @Test
    public void delegatePutOnNotFound() throws Exception {
        String randomName = "user1_" + UUIDUtils.newTimeUUID().toString();
        createUser( randomName, randomName + "@apigee.com", "password", randomName );

        // should update a field
        JsonNode response = resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/" + randomName )
                .queryParam( "access_token", adminAccessToken ).accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON_TYPE ).get( JsonNode.class );
        logNode( response );
        assertNotNull( getEntity( response, 0 ) );
        // PUT on user

        // PUT a new user
        randomName = "user2_" + UUIDUtils.newTimeUUID().toString();
        Map<String, String> payload =
                hashMap( "email", randomName + "@apigee.com" ).map( "username", randomName ).map( "name", randomName )
                        .map( "password", "password" ).map( "pin", "1234" );

        response = resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users" ).queryParam( "access_token", adminAccessToken )
                .accept( MediaType.APPLICATION_JSON ).type( MediaType.APPLICATION_JSON_TYPE )
                .put( JsonNode.class, payload );

        logNode( response );
        response = resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/" + randomName )
                .queryParam( "access_token", adminAccessToken ).accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON_TYPE ).get( JsonNode.class );

        assertNotNull( getEntity( response, 0 ) );
        logNode( response );
    }


    /**
     * Test that property queries return properties and entity queries return entities.
     * https://apigeesc.atlassian.net/browse/USERGRID-1715?
     */
    @Test
    public void queryForUuids() throws Exception {

        {
            final JsonNode response = resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/" )
                    .queryParam( "ql", "select *" )               // query for entities
                    .queryParam( "access_token", access_token ).accept( MediaType.APPLICATION_JSON )
                    .type( MediaType.APPLICATION_JSON_TYPE ).get( JsonNode.class );
            assertNotNull( "Entities must exist", response.get( "entities" ) );
            assertTrue( "Must be some entities", response.get( "entities" ).size() > 0 );
            assertEquals( "Must be users", "user", response.get( "entities" ).get( 0 ).get( "type" ).asText() );
            assertNull( "List must not exist", response.get( "list" ) );
        }

        {
            final JsonNode response = resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/" )
                    .queryParam( "ql", "select uuid" )            // query for uuid properties
                    .queryParam( "access_token", access_token ).accept( MediaType.APPLICATION_JSON )
                    .type( MediaType.APPLICATION_JSON_TYPE ).get( JsonNode.class );
            assertNotNull( "List must exist", response.get( "list" ) );
            assertTrue( "Must be some list items", response.get( "list" ).size() > 0 );
            assertNull( "Entities must not exist", response.get( "entries" ) );
        }
    }
}
