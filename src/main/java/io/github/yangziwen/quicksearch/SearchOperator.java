package io.github.yangziwen.quicksearch;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import io.github.yangziwen.quickdao.core.Criterion;
import io.github.yangziwen.quickdao.core.Operator;

public enum SearchOperator {

    eq {
        @Override
        public QueryBuilder generateQueryBuilder(Criterion<?> criterion) {
            return QueryBuilders.termQuery(criterion.getQueryName(), criterion.getValue());
        }
    },

    ne {
        @Override
        public QueryBuilder generateQueryBuilder(Criterion<?> criterion) {
            return QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery(criterion.getQueryName(), criterion.getValue()));
        }
    },

    gt {
        @Override
        public QueryBuilder generateQueryBuilder(Criterion<?> criterion) {
            return QueryBuilders.rangeQuery(criterion.getQueryName()).gt(criterion.getValue());
        }
    },

    ge {
        @Override
        public QueryBuilder generateQueryBuilder(Criterion<?> criterion) {
            return QueryBuilders.rangeQuery(criterion.getQueryName()).gte(criterion.getValue());
        }
    },

    lt {
        @Override
        public QueryBuilder generateQueryBuilder(Criterion<?> criterion) {
            return QueryBuilders.rangeQuery(criterion.getQueryName()).lt(criterion.getValue());
        }
    },

    le {
        @Override
        public QueryBuilder generateQueryBuilder(Criterion<?> criterion) {
            return QueryBuilders.rangeQuery(criterion.getQueryName()).lte(criterion.getValue());
        }
    },

    contain {
        @Override
        public QueryBuilder generateQueryBuilder(Criterion<?> criterion) {
            return null;
        }
    },

    not_contain {
        @Override
        public QueryBuilder generateQueryBuilder(Criterion<?> criterion) {
            return null;
        }
    },

    start_with {
        @Override
        public QueryBuilder generateQueryBuilder(Criterion<?> criterion) {
            return null;
        }
    },

    not_start_with {
        @Override
        public QueryBuilder generateQueryBuilder(Criterion<?> criterion) {
            return null;
        }
    },

    end_with {
        @Override
        public QueryBuilder generateQueryBuilder(Criterion<?> criterion) {
            return null;
        }
    },

    not_end_with {
        @Override
        public QueryBuilder generateQueryBuilder(Criterion<?> criterion) {
            return null;
        }
    },

    in {
        @Override
        public QueryBuilder generateQueryBuilder(Criterion<?> criterion) {
            return null;
        }
    },

    not_in {
        @Override
        public QueryBuilder generateQueryBuilder(Criterion<?> criterion) {
            return null;
        }
    },

    is_null {
        @Override
        public QueryBuilder generateQueryBuilder(Criterion<?> criterion) {
            return null;
        }
    },

    is_not_null {
        @Override
        public QueryBuilder generateQueryBuilder(Criterion<?> criterion) {
            // TODO Auto-generated method stub
            return null;
        }
    },

    impossible {
        @Override
        public QueryBuilder generateQueryBuilder(Criterion<?> criterion) {
            // TODO Auto-generated method stub
            return null;
        }
    };


    private static final ConcurrentMap<Operator, SearchOperator> OPERATOR_MAPPING = new ConcurrentHashMap<Operator, SearchOperator>() {
        private static final long serialVersionUID = 1L;
        {
            for (Operator operator : Operator.values()) {
                for (SearchOperator searchOperator : SearchOperator.values()) {
                    if (StringUtils.equals(operator.name(),searchOperator.name())) {
                        put(operator, searchOperator);
                    }
                }
            }
        }
    };

    public static SearchOperator from(Operator operator) {
        return OPERATOR_MAPPING.get(operator);
    }

    public abstract QueryBuilder generateQueryBuilder(Criterion<?> criterion);

}
