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
package org.usergrid.services;

import static org.usergrid.utils.ClassUtils.cast;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.usergrid.persistence.Entity;
import org.usergrid.persistence.EntityRef;
import org.usergrid.persistence.Query;
import org.usergrid.persistence.Results;
import org.usergrid.persistence.Results.Level;
import org.usergrid.persistence.Schema;
import org.usergrid.services.ServiceResults.Type;
import org.usergrid.services.exceptions.ServiceResourceNotFoundException;

public class AbstractCollectionService extends AbstractService {

	private static final Logger logger = LoggerFactory
			.getLogger(AbstractCollectionService.class);

	public AbstractCollectionService() {
		// addSet("indexes");
		declareMetadataType("indexes");
	}

	// cname/id/

	@Override
	public Entity getEntity(ServiceRequest request, UUID uuid) throws Exception {
		if (!isRootService()) {
			return null;
		}
		Entity entity = em.get(uuid);
		if (entity != null) {
			entity = importEntity(request, entity);
		}
		return entity;
	}

	@Override
	public Entity getEntity(ServiceRequest request, String name)
			throws Exception {
		if (!isRootService()) {
			return null;
		}
		String nameProperty = Schema.getDefaultSchema().aliasProperty(
				getEntityType());
		if (nameProperty == null) {
			nameProperty = "name";
		}

		EntityRef entityRef = em.getAlias(getEntityType(), name);
		if (entityRef == null) {
			return null;
		}
		Entity entity = em.get(entityRef);
		if (entity != null) {
			entity = importEntity(request, entity);
		}
		return entity;
	}

	@Override
	public ServiceResults getItemById(ServiceContext context, UUID id)
			throws Exception {

		EntityRef entity = null;

		if (!context.moreParameters()) {
			entity = em.get(id);

			entity = importEntity(context, (Entity) entity);
		} else {
			entity = em.getRef(id);
		}

		if (entity == null) {
			throw new ServiceResourceNotFoundException(context);
		}

		checkPermissionsForEntity(context, entity);

		// TODO check that entity is in fact in the collection

		List<ServiceRequest> nextRequests = context
				.getNextServiceRequests(entity);

		return new ServiceResults(this, context, Type.COLLECTION,
				Results.fromRef(entity), null, nextRequests);
	}

	@Override
	public ServiceResults getItemByName(ServiceContext context, String name)
			throws Exception {

		String nameProperty = Schema.getDefaultSchema().aliasProperty(
				getEntityType());
		if (nameProperty == null) {
			nameProperty = "name";
		}

		EntityRef entity = em.getAlias(getEntityType(), name);
		if (entity == null) {
			throw new ServiceResourceNotFoundException(context);
		}

		if (!context.moreParameters()) {
			entity = em.get(entity);
			entity = importEntity(context, (Entity) entity);
		}

		checkPermissionsForEntity(context, entity);

		/*
		 * Results.Level level = Results.Level.REFS; if (isEmpty(parameters)) {
		 * level = Results.Level.ALL_PROPERTIES; }
		 * 
		 * Results results = em.searchCollectionForProperty(owner,
		 * getCollectionName(), null, nameProperty, name, null, null, 1, level);
		 * EntityRef entity = results.getRef();
		 */

		List<ServiceRequest> nextRequests = context
				.getNextServiceRequests(entity);

		return new ServiceResults(this, context, Type.COLLECTION,
				Results.fromRef(entity), null, nextRequests);
	}

	@Override
	public ServiceResults getItemsByQuery(ServiceContext context, Query query)
			throws Exception {

		checkPermissionsForCollection(context);

		int count = 1;
		Results.Level level = Results.Level.REFS;

		if (!context.moreParameters()) {
			count = 0;
			level = Results.Level.ALL_PROPERTIES;
		}

		if (context.getRequest().isReturnsTree()) {
			level = Results.Level.ALL_PROPERTIES;
		}

		query = new Query(query);
		query.setResultsLevel(level);
		query.setLimit(query.getLimit(count));
		if (!query.isReversedSet()) {
			query.setReversed(isCollectionReversed(context));
		}
		query.addSort(getCollectionSort(context));
		/*
		 * if (count > 0) { query.setMaxResults(count); }
		 */

		Results r = em.searchCollection(context.getOwner(),
				context.getCollectionName(), query);

		List<ServiceRequest> nextRequests = null;
		if (!r.isEmpty()) {

			if (!context.moreParameters()) {
				importEntities(context, r);
			}

			nextRequests = context.getNextServiceRequests(r.getRefs());
		}

		return new ServiceResults(this, context, Type.COLLECTION, r, null,
				nextRequests);
	}

	@Override
	public ServiceResults getCollection(ServiceContext context)
			throws Exception {

		checkPermissionsForCollection(context);

		if (getCollectionSort(context) != null) {
			return getItemsByQuery(context, new Query());
		}

		int count = 10;
		Results r = em.getCollection(context.getOwner(),
				context.getCollectionName(), null, count, Level.ALL_PROPERTIES,
				isCollectionReversed(context));

		importEntities(context, r);

		/*
		 * if (r.isEmpty()) { throw new
		 * ServiceResourceNotFoundException(request); }
		 */

		return new ServiceResults(this, context, Type.COLLECTION, r, null, null);
	}

	@Override
	public ServiceResults putItemById(ServiceContext context, UUID id)
			throws Exception {

		if (context.moreParameters()) {
			return getItemById(context, id);
		}

		checkPermissionsForEntity(context, id);

		Entity item = em.get(id);
		if (item != null) {
			updateEntity(context, item, context.getPayload());
			item = importEntity(context, item);
		} else {
			String entityType = getEntityType();
			item = em.create(id, entityType, context.getPayload()
					.getProperties());
		}

		return new ServiceResults(this, context, Type.COLLECTION,
				Results.fromEntity(item), null, null);
	}

	@Override
	public ServiceResults putItemByName(ServiceContext context, String name)
			throws Exception {

		if (context.moreParameters()) {
			return getItemByName(context, name);
		}

		EntityRef ref = em.getAlias(getEntityType(), name);
		if (ref == null) {
			throw new ServiceResourceNotFoundException(context);
		}
		Entity entity = em.get(ref);
		entity = importEntity(context, entity);

		checkPermissionsForEntity(context, entity);

		updateEntity(context, entity);

		return new ServiceResults(this, context, Type.COLLECTION,
				Results.fromEntity(entity), null, null);

	}

	@Override
	public ServiceResults putItemsByQuery(ServiceContext context, Query query)
			throws Exception {

		checkPermissionsForCollection(context);

		if (context.moreParameters()) {
			return getItemsByQuery(context, query);
		}

		query = new Query(query);
		query.setResultsLevel(Level.ALL_PROPERTIES);
		query.setLimit(1000);
		if (!query.isReversedSet()) {
			query.setReversed(isCollectionReversed(context));
		}
		query.addSort(getCollectionSort(context));

		Results r = em.searchCollection(context.getOwner(),
				context.getCollectionName(), query);
		if (r.isEmpty()) {
			throw new ServiceResourceNotFoundException(context);
		}

		updateEntities(context, r);

		return new ServiceResults(this, context, Type.COLLECTION, r, null, null);
	}

	@Override
	public ServiceResults postCollection(ServiceContext context)
			throws Exception {

		checkPermissionsForCollection(context);

		if (context.getPayload().isBatch()) {
			List<Entity> entities = new ArrayList<Entity>();
			List<Map<String, Object>> batch = context.getPayload()
					.getBatchProperties();
			logger.info("Attempting to batch create " + batch.size()
					+ " entities in collection " + context.getCollectionName());
			int i = 1;
			for (Map<String, Object> p : batch) {
				logger.info("Creating entity " + i + " in collection "
						+ context.getCollectionName());

				Entity item = null;

				try {
					item = em.createItemInCollection(context.getOwner(),
							context.getCollectionName(), getEntityType(), p);
				} catch (Exception e) {
					logger.error(
							"Entity " + i
									+ " unable to be created in collection "
									+ context.getCollectionName(), e);

					i++;
					continue;
				}

				logger.info("Entity " + i + " created in collection "
						+ context.getCollectionName() + " with UUID "
						+ item.getUuid());

				item = importEntity(context, item);
				entities.add(item);
				i++;
			}
			return new ServiceResults(this, context, Type.COLLECTION,
					Results.fromEntities(entities), null, null);
		}

		Entity item = em.createItemInCollection(context.getOwner(),
				context.getCollectionName(), getEntityType(),
				context.getProperties());

		item = importEntity(context, item);

		return new ServiceResults(this, context, Type.COLLECTION,
				Results.fromEntity(item), null, null);

	}

	@Override
	public ServiceResults postItemsByQuery(ServiceContext context, Query query)
			throws Exception {
		if (context.moreParameters()) {
			return super.postItemsByQuery(context, query);
		}
		return postCollection(context);
	}

	@Override
	public ServiceResults postItemById(ServiceContext context, UUID id)
			throws Exception {

		checkPermissionsForEntity(context, id);

		if (context.moreParameters()) {
			return getItemById(context, id);
		}

		Entity entity = em.get(id);
		if (entity == null) {
			throw new ServiceResourceNotFoundException(context);
		}
		entity = importEntity(context, entity);

		em.addToCollection(context.getOwner(), context.getCollectionName(),
				entity);

		return new ServiceResults(null, context, Type.COLLECTION,
				Results.fromEntity(entity), null, null);
	}

	@Override
	public ServiceResults postItemByName(ServiceContext context, String name)
			throws Exception {

		if (context.moreParameters()) {
			return super.postItemByName(context, name);
		}

		EntityRef ref = em.getAlias(getEntityType(), name);
		if (ref == null) {
			throw new ServiceResourceNotFoundException(context);
		}

		return postItemById(context, ref.getUuid());
	}

	@Override
	public ServiceResults deleteItemById(ServiceContext context, UUID id)
			throws Exception {

		checkPermissionsForEntity(context, id);

		if (context.moreParameters()) {
			return getItemById(context, id);
		}

		Entity item = em.get(id);
		if (item == null) {
			throw new ServiceResourceNotFoundException(context);
		}
		item = importEntity(context, item);

		em.removeFromCollection(context.getOwner(),
				context.getCollectionName(), item);

		return new ServiceResults(this, context, Type.COLLECTION,
				Results.fromEntity(item), null, null);

	}

	@Override
	public ServiceResults deleteItemByName(ServiceContext context, String name)
			throws Exception {

		if (context.moreParameters()) {
			return getItemByName(context, name);
		}

		EntityRef ref = em.getAlias(getEntityType(), name);
		if (ref == null) {
			throw new ServiceResourceNotFoundException(context);
		}
		Entity entity = em.get(ref);
		if (entity == null) {
			throw new ServiceResourceNotFoundException(context);
		}
		entity = importEntity(context, entity);

		checkPermissionsForEntity(context, entity);

		em.removeFromCollection(context.getOwner(),
				context.getCollectionName(), entity);

		return new ServiceResults(this, context, Type.COLLECTION,
				Results.fromEntity(entity), null, null);

	}

	@Override
	public ServiceResults deleteItemsByQuery(ServiceContext context, Query query)
			throws Exception {

		checkPermissionsForCollection(context);

		if (context.moreParameters()) {
			return getItemsByQuery(context, query);
		}

		query = new Query(query);
		query.setResultsLevel(Level.ALL_PROPERTIES);
		query.setLimit(1000);
		if (!query.isReversedSet()) {
			query.setReversed(isCollectionReversed(context));
		}
		query.addSort(getCollectionSort(context));

		Results r = em.searchCollection(context.getOwner(),
				context.getCollectionName(), query);
		if (r.isEmpty()) {
			throw new ServiceResourceNotFoundException(context);
		}

		importEntities(context, r);

		for (Entity entity : r) {
			em.removeFromCollection(context.getOwner(),
					context.getCollectionName(), entity);
		}

		return new ServiceResults(this, context, Type.COLLECTION, r, null, null);
	}

	@Override
	public ServiceResults getServiceMetadata(ServiceContext context,
			String metadataType) throws Exception {

		if ("indexes".equals(metadataType)) {
			Set<String> indexes = cast(em.getCollectionIndexes(
					context.getOwner(), context.getCollectionName()));

			return new ServiceResults(this, context.getRequest().withPath(
					context.getRequest().getPath() + "/indexes"),
					context.getPreviousResults(), context.getChildPath(),
					Type.GENERIC, Results.fromData(indexes), null, null);
		}
		return null;

	}

}