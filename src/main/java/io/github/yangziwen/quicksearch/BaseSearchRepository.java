package io.github.yangziwen.quicksearch;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.persistence.PersistenceException;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
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
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation.SingleValue;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import com.alibaba.fastjson.JSON;

import io.github.yangziwen.quickdao.core.BaseReadOnlyRepository;
import io.github.yangziwen.quickdao.core.Criteria;
import io.github.yangziwen.quickdao.core.Criterion;
import io.github.yangziwen.quickdao.core.EntityMeta;
import io.github.yangziwen.quickdao.core.FunctionStmt;
import io.github.yangziwen.quickdao.core.Order;
import io.github.yangziwen.quickdao.core.Order.Direction;
import io.github.yangziwen.quickdao.core.Query;
import io.github.yangziwen.quickdao.core.RepoKeys;
import io.github.yangziwen.quickdao.core.Stmt;
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
        boolean hasGroupBy = CollectionUtils.isNotEmpty(query.getGroupByList());
        boolean hasFuncStmt = query.getSelectStmtList().stream()
                .anyMatch(FunctionStmt.class::isInstance);
        if (!hasGroupBy && !hasFuncStmt) {
            return doListQuery(query);
        }
        else if (!hasGroupBy && hasFuncStmt) {
            return doListAggs(query);
        }
        else if (hasGroupBy) {
            return doListBucketAggs(query);
        }
        else {
            throw new RuntimeException("group by operation without func stmt is not supported, query is " + query);
        }
    }

    private List<E> doListAggs(Query query) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .query(generateQueryBuilder(query.getCriteria()))
                .from(0)
                .size(0);
        List<AggregationBuilder> aggsBuilderList = query.getSelectStmtList().stream()
                .filter(FunctionStmt.class::isInstance)
                .map(FunctionStmt.class::cast)
                .map(SearchFunctionEnum::generateAggsBuilder)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        for (AggregationBuilder aggsBuilder : aggsBuilderList) {
            sourceBuilder.aggregation(aggsBuilder);
        }
        SearchRequest request = new SearchRequest(entityMeta.getTable());
        request.source(sourceBuilder);
        try {
            SearchResponse response = client.search(request, options);
            Aggregations aggs = response.getAggregations();
            Map<String, Object> resultMap = aggs.asList()
                    .stream()
                    .collect(Collectors.toMap(
                            Aggregation::getName,
                            agg -> NumericMetricsAggregation.SingleValue.class.cast(agg).value()));
            E entity = JSON.parseObject(JSON.toJSONString(resultMap), entityMeta.getClassType());
            return Collections.singletonList(entity);
        } catch (IOException e) {
            throw new RuntimeException("failed to list aggregation of type " + entityMeta.getClassType().getName() + " by " + query, e);
        }
    }

    private List<E> doListBucketAggs(Query query) {

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .query(generateQueryBuilder(query.getCriteria()))
                .from(0)
                .size(0);

        List<FunctionStmt<?>> funcStmtList = query.getSelectStmtList().stream()
                .filter(FunctionStmt.class::isInstance)
                .map(FunctionStmt.class::cast)
                .collect(Collectors.toList());

        AggregationBuilder outterAggsBuilder = null;
        AggregationBuilder innerAggsBuilder = null;
        Map<String, TermsAggregationBuilder> termsAggsBuilderMap = new HashMap<>();
        for (String groupBy : query.getGroupByList()) {
            TermsAggregationBuilder aggsBuilder = AggregationBuilders.terms(groupBy.replaceFirst("\\.keyword$", "")).field(groupBy);
            termsAggsBuilderMap.put(groupBy, aggsBuilder);
            if (outterAggsBuilder == null) {
                outterAggsBuilder = aggsBuilder;
            }
            if (innerAggsBuilder != null) {
                innerAggsBuilder.subAggregation(aggsBuilder);
            }
            innerAggsBuilder = aggsBuilder;
        }

        List<AggregationBuilder> statsAggsBuilderList = funcStmtList.stream()
                .map(SearchFunctionEnum::generateAggsBuilder)
                .collect(Collectors.toList());
        for (AggregationBuilder statsAggs : statsAggsBuilderList) {
            innerAggsBuilder.subAggregation(statsAggs);
        }

        if (CollectionUtils.isNotEmpty(query.getOrderList())) {
            for (Order order : query.getOrderList()) {
                TermsAggregationBuilder termsAggsBuilder = termsAggsBuilderMap.get(order.getName());
                if (termsAggsBuilder != null) {
                    termsAggsBuilder.order(BucketOrder.key(order.getDirection() == Direction.ASC));
                }
            }
        }

        sourceBuilder.aggregation(outterAggsBuilder);

        SearchRequest request = new SearchRequest(entityMeta.getTable());

        request.source(sourceBuilder);

        try {
            SearchResponse response = client.search(request, options);
            List<Map<String, Object>> resultList = walkAggregations(response.getAggregations());
            List<E> entities = new ArrayList<>(resultList.size());
            for (Map<String, Object> result : resultList) {
                entities.add(JSON.parseObject(JSON.toJSONString(result), entityMeta.getClassType()));
            }
            return entities;
        } catch (IOException e) {
            throw new RuntimeException("failed to list aggregation of type " + entityMeta.getClassType().getName() + " by " + query, e);
        }
    }

    private List<Map<String, Object>> walkAggregations(Aggregations aggregations) {
        if (aggregations == null) {
            return Collections.emptyList();
        }
        List<Aggregation> aggregationList = aggregations.asList();
        List<Map<String, Object>> resultList = new ArrayList<>();
        for (Aggregation aggregation : aggregationList) {
            if (aggregation instanceof ParsedTerms) {
                String key = aggregation.getName();
                List<? extends Terms.Bucket> bucketList = ParsedTerms.class.cast(aggregation).getBuckets();
                for (Terms.Bucket bucket : bucketList) {
                    Object value = bucket.getKey();
                    List<Map<String, Object>> mapList = walkAggregations(bucket.getAggregations());
                    for (Map<String, Object> map : mapList) {
                        map.put(key, value);
                        map.putIfAbsent("count", bucket.getDocCount());
                    }
                    resultList.addAll(mapList);
                }
            } else {
                break;
            }
        }
        Map<String, Object> result = new HashMap<>();
        for (Aggregation aggregation : aggregationList) {
            if (aggregation instanceof NumericMetricsAggregation.SingleValue) {
                SingleValue valueObj = NumericMetricsAggregation.SingleValue.class.cast(aggregation);
                result.put(valueObj.getName(), valueObj.value());
            } else {
                break;
            }
        }
        if (MapUtils.isNotEmpty(result)) {
            resultList.add(result);
        }
        return resultList;
    }

    private List<E> doListQuery(Query query) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .query(generateQueryBuilder(query.getCriteria()))
                .from(query.getOffset())
                .size(Math.min(query.getLimit(), getDefaultMaxSize()));
        List<Stmt> stmtList = query.getSelectStmtList()
                .stream()
                .filter(stmt -> !FunctionStmt.class.isInstance(stmt))
                .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(stmtList)) {
            List<String> includes = stmtList.stream()
                    .map(Stmt::getField)
                    .collect(Collectors.toList());
            sourceBuilder.fetchSource(includes.toArray(ArrayUtils.EMPTY_STRING_ARRAY), ArrayUtils.EMPTY_STRING_ARRAY);
        }
        if (CollectionUtils.isNotEmpty(query.getOrderList())) {
            for (Order order : query.getOrderList()) {
                sourceBuilder.sort(generateSortBuilder(order));
            }
        }
        SearchRequest request = new SearchRequest(entityMeta.getTable());
        request.source(sourceBuilder);
        try {
            List<E> entities = new ArrayList<>();
            SearchResponse response = client.search(request, options);
            for (SearchHit hit : response.getHits().getHits()) {
                E entity = JSON.parseObject(hit.getSourceAsString(), entityMeta.getClassType());
                Field idField = entityMeta.getIdField();
                if (idField != null) {
                    BeanMap.create(entity).put(idField.getName(), hit.getId());
                }
                entities.add(entity);
            }
            return entities;
        } catch (IOException e) {
            throw new RuntimeException("failed to list entity of type " + entityMeta.getClassType().getName() + " by " + query, e);
        }
    }

    @Override
    public Integer count(Query query) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .query(generateQueryBuilder(query.getCriteria()));
        CountRequest request = new CountRequest(entityMeta.getTable());
        request.source(sourceBuilder);
        try {
            CountResponse response = client.count(request, options);
            return Long.valueOf(response.getCount()).intValue();
        } catch (IOException e) {
            throw new RuntimeException("failed to count entity of type " + entityMeta.getClassType().getName() + " by " + query, e);
        }
    }

    private SortBuilder<?> generateSortBuilder(Order order) {
        SortOrder sortOrder = SortOrder.ASC;
        if (order.getDirection() == Direction.DESC) {
            sortOrder = SortOrder.DESC;
        }
        return SortBuilders.fieldSort(order.getName()).order(sortOrder);
    }

    private QueryBuilder generateQueryBuilder(Criteria criteria) {
        if (criteria.isEmpty()) {
            return QueryBuilders.matchAllQuery();
        }
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        for (Criterion<?> criterion : criteria.getCriterionList()) {
            SearchOperator operator = SearchOperator.from(criterion.getOperator());
            if (operator == null) {
                continue;
            }
            boolQueryBuilder.must(operator.generateQueryBuilder(criterion));
        }
        for (Entry<String, Criteria> entry : criteria.getNestedCriteriaMap().entrySet()) {
            if (entry.getKey().endsWith(RepoKeys.AND)) {
                boolQueryBuilder.must(generateQueryBuilder(entry.getValue()));
            }
            if (entry.getKey().endsWith(RepoKeys.OR)) {
                boolQueryBuilder.should(generateQueryBuilder(entry.getValue()));
            }
        }
        return boolQueryBuilder;
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

    public int deleteByIds(Collection<?> ids) {
        if (CollectionUtils.isEmpty(ids)) {
            return 0;
        }
        BulkRequest request = new BulkRequest();
        for (Object id : ids) {
            request.add(new DeleteRequest(entityMeta.getTable(), String.valueOf(id)));
        }
        try {
            int result = ids.size();
            BulkResponse response = client.bulk(request, options);
            for (BulkItemResponse item : response.getItems()) {
                if (item.isFailed()) {
                    result --;
                }
            }
            return result;
        } catch (IOException e) {
            throw new PersistenceException("faield to delete entities of type " + entityMeta.getClassType().getName(), e);
        }
    }

    @SuppressWarnings("unchecked")
    private <K, V> Map<K, V> createBeanMap(E entity) {
        if (entity == null) {
            return Collections.emptyMap();
        }
        return BeanMap.create(entity);
    }

    protected int getDefaultMaxSize() {
        return 10000;
    }

}
