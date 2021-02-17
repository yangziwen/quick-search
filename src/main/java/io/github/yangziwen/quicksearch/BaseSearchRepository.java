package io.github.yangziwen.quicksearch;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.persistence.PersistenceException;

import org.apache.commons.collections4.CollectionUtils;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetRequest.Item;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import com.alibaba.fastjson.JSON;

import io.github.yangziwen.quickdao.core.BaseReadOnlyRepository;
import io.github.yangziwen.quickdao.core.Criteria;
import io.github.yangziwen.quickdao.core.EntityMeta;
import io.github.yangziwen.quickdao.core.Query;
import io.github.yangziwen.quickdao.core.TypedCriteria;
import io.github.yangziwen.quickdao.core.TypedQuery;
import io.github.yangziwen.quickdao.core.util.ReflectionUtil;
import net.sf.cglib.beans.BeanMap;

public abstract class BaseSearchRepository<E> implements BaseReadOnlyRepository<E> {

    protected final EntityMeta<E> entityMeta;

    protected final RestHighLevelClient client;

    protected final RequestOptions options;

    protected BaseSearchRepository(RestHighLevelClient client) {
        this(client, RequestOptions.DEFAULT);
    }

    protected BaseSearchRepository(RestHighLevelClient client, RequestOptions options) {
        this.entityMeta = EntityMeta.newInstance(ReflectionUtil.<E> getSuperClassGenericType(this.getClass(), 0));
        this.client = client;
        this.options = options;
    }

    @Override
    public E getById(Object id) {
        try {
            GetRequest request = new GetRequest(entityMeta.getTable(), String.valueOf(id));
            GetResponse response = client.get(request, options);
            return extractEntityFromGetResponse(response);
        } catch (IOException e) {
            throw new RuntimeException("failed to query entity of type " + entityMeta.getClassType().getName() + " by id " + id, e);
        }
    }

    @Override
    public List<E> listByIds(Collection<?> ids) {
        if (CollectionUtils.isEmpty(ids)) {
            return Collections.emptyList();
        }
        MultiGetRequest request = new MultiGetRequest();
        for (Object id : ids) {
            request.add(new Item(entityMeta.getTable(), String.valueOf(id)));
        }
        try {
            List<E> entities = new ArrayList<>(ids.size());
            MultiGetResponse response = client.mget(request, options);
            for (MultiGetItemResponse item : response.getResponses()) {
                entities.add(extractEntityFromGetResponse(item.getResponse()));

            }
            return entities;
        } catch (IOException e) {
            throw new RuntimeException("failed to query entity of type " + entityMeta.getClassType().getName() + " by ids " + ids, e);
        }

    }

    private E extractEntityFromGetResponse(GetResponse response) {
        if (!response.isExists()) {
            return null;
        }
        E entity = JSON.parseObject(response.getSourceAsString(), entityMeta.getClassType());
        Field idField = entityMeta.getIdField();
        if (idField != null) {
            BeanMap.create(entity).put(idField.getName(), response.getId());
        }
        return entity;
    }

    @Override
    public List<E> list(Query query) {
        return null;
    }

    @Override
    public Integer count(Query query) {
        return null;
    }

    @Override
    public TypedCriteria<E> newTypedCriteria() {
        return new TypedCriteria<>(entityMeta.getClassType());
    }

    @Override
    public TypedQuery<E> newTypedQuery() {
        return new TypedQuery<>(entityMeta.getClassType());
    }

    public int insert(E entity) {

        Map<String, Object> beanMap = createBeanMap(entity);

        IndexRequest request = generateIndexRequest(beanMap);

        try {
            IndexResponse response = client.index(request, options);
            if (entityMeta.getIdField() != null && entityMeta.getIdGeneratedValue() != null) {
                beanMap.put(entityMeta.getIdFieldName(), response.getId());
            }
            return response.getResult() == Result.CREATED ? 1 : 0;
        } catch (IOException e) {
           throw new PersistenceException("faield to persist entity of type " + entityMeta.getClassType().getName(), e);
        }
    }

    public int batchInsert(List<E> entities, int batchSize) {

        if (CollectionUtils.isEmpty(entities)) {
            return 0;
        }

        for (int i = 0; i < entities.size(); i += batchSize) {
            List<E> sublist = entities.subList(i, Math.min(i + batchSize, entities.size()));
            List<BeanMap> beanMapList = sublist.stream()
                    .map(BeanMap::create)
                    .collect(Collectors.toList());
            BulkRequest request = new BulkRequest();
            for (BeanMap beanMap : beanMapList) {
                request.add(generateIndexRequest(beanMap));
            }
            try {
                BulkResponse response = client.bulk(request, options);
                if (entityMeta.getIdField() != null && entityMeta.getIdGeneratedValue() != null) {
                    for (int j = 0; j < response.getItems().length; j++) {
                        String idVal = response.getItems()[j].getId();
                        beanMapList.get(j).put(entityMeta.getIdFieldName(), idVal);
                    }
                }
            } catch (IOException e) {
                throw new PersistenceException("faield to persist entities of type " + entityMeta.getClassType().getName(), e);
            }
        }

        return entities.size();
    }

    private IndexRequest generateIndexRequest(Map<String, Object> beanMap) {

        Map<String, Object> entityMap = new HashMap<>();

        for (Field field : entityMeta.getFieldsWithoutIdField()) {
            entityMap.put(field.getName(), beanMap.get(field.getName()));
        }

        IndexRequest request = new IndexRequest(entityMeta.getTable());

        if (entityMeta.getIdField() != null && entityMeta.getIdGeneratedValue() == null) {
            Object idVal = beanMap.get(entityMeta.getIdField().getName());
            if (idVal == null) {
                throw new IllegalStateException("failed to get id of entity[" + beanMap + "]");
            }
            request.id(String.valueOf(idVal));
        }

        request.source(entityMap);

        return request;
    }

    public int update(E entity) {

        Map<String, Object> beanMap = createBeanMap(entity);

        Map<String, Object> entityMap = new HashMap<>();

        for (Field field : entityMeta.getFieldsWithoutIdField()) {
            entityMap.put(field.getName(), beanMap.get(field.getName()));
        }

        Object idVal = beanMap.get(entityMeta.getIdFieldName());

        UpdateRequest request = new UpdateRequest(entityMeta.getTable(), String.valueOf(idVal));

        request.doc(entityMap);

        try {
            UpdateResponse response = client.update(request, options);
            return response.getResult() == Result.UPDATED ? 1 : 0;
        } catch (IOException e) {
            throw new PersistenceException("faield to update entity of type " + entityMeta.getClassType().getName(), e);
        }
    }

    public int updateSelective(E entity) {

        Map<String, Object> beanMap = createBeanMap(entity);

        Map<String, Object> entityMap = new HashMap<>();

        for (Field field : entityMeta.getFieldsWithoutIdField()) {
            Object value = beanMap.get(field.getName());
            if (value == null) {
                continue;
            }
            entityMap.put(field.getName(), value);
        }

        Object idVal = beanMap.get(entityMeta.getIdFieldName());

        UpdateRequest request = new UpdateRequest(entityMeta.getTable(), String.valueOf(idVal));

        request.doc(entityMap);

        try {
            UpdateResponse response = client.update(request, options);
            return response.getResult() == Result.UPDATED ? 1 : 0;
        } catch (IOException e) {
            throw new PersistenceException("faield to update entity of type " + entityMeta.getClassType().getName(), e);
        }

    }

    public int deleteById(Object id) {

        if (id == null) {
            return 0;
        }

        DeleteRequest request = new DeleteRequest(entityMeta.getTable(), String.valueOf(id));

        DeleteResponse response;
        try {
            response = client.delete(request, options);
            return response.getResult() == Result.DELETED ? 1 : 0;
        } catch (IOException e) {
            throw new PersistenceException("faield to delete entity of type " + entityMeta.getClassType().getName(), e);
        }
    }

    @SuppressWarnings("unchecked")
    private <K, V> Map<K, V> createBeanMap(E entity) {
        if (entity == null) {
            return Collections.emptyMap();
        }
        return BeanMap.create(entity);
    }

}
