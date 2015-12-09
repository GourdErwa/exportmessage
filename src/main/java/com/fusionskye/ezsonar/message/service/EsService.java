package com.fusionskye.ezsonar.message.service;

import com.fusionskye.ezsonar.message.dao.ESClient;
import com.fusionskye.ezsonar.message.model.*;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.terms.InternalTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.InternalStats;

import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.elasticsearch.common.Preconditions.checkArgument;
import static org.elasticsearch.common.Preconditions.checkNotNull;

/**
 * @author wei.Li by 15/12/7
 */
public final class EsService {

    public static final String TIME_FIELD_NAME = "_start_at",
            STREAMS_FIELD_NAME = "streams",
            LATENCY_MSEC_FIELD_NAME = "_latency_msec",
            COUNT_FIELD_NAME = "count",
            RESPONSE_FIELD_NAME = "response";

    //默认统计的字段
    public static final ImmutableMultimap<String, String> DEFAULT_STATISTICS_FIELD_MAP = ImmutableMultimap.of(
            COUNT_FIELD_NAME, "交易数量",
            LATENCY_MSEC_FIELD_NAME, "响应时间",
            RESPONSE_FIELD_NAME, "响应数量"
    );


    //索引名称后缀格式
    private static final DateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    //待统计分组字段 <key,别名>
    private static final Map<String, String> STATISTICS_GROUP_FIELD_MAP = Maps.newTreeMap();
    private static final NumberFormat NUMBER_FORMAT;
    private static TermsBuilder BUILDERS = null;

    static {
        //小数点保留 2 位格式化
        NUMBER_FORMAT = NumberFormat.getNumberInstance();
        NUMBER_FORMAT.setMaximumFractionDigits(2);
        NUMBER_FORMAT.setGroupingUsed(false);
    }

    private CoreService coreService;

    public EsService(CoreService coreService) {
        this.coreService = coreService;
        analyzerStatisticsGroupField();
    }

    public static Map<String, String> getStatisticsGroupFieldMap() {
        return STATISTICS_GROUP_FIELD_MAP;
    }

    /**
     * 解析统计的字段,拼接 AggregationBuilders
     */
    private void analyzerStatisticsGroupField() {

        String fieldValues = this.coreService.getSystemProperties().getStatisticalFieldValues();

        checkNotNull(fieldValues);
        fieldValues = fieldValues.replaceAll("\\s", "");
        final String[] split = fieldValues.split(",");

        Map<String, String> map = Maps.newTreeMap();
        for (String s : split) {
            final String[] split1 = s.split("\\|");
            checkArgument(split1.length == 2, "统计字段" + s + " 配置格式错误");
            map.put(split1[0], split1[1]);
        }

        synchronized (STATISTICS_GROUP_FIELD_MAP) {
            STATISTICS_GROUP_FIELD_MAP.clear();
            STATISTICS_GROUP_FIELD_MAP.putAll(map);
        }

        List<TermsBuilder> termsBuilders = Lists.newArrayList();
        final Set<String> keySet = STATISTICS_GROUP_FIELD_MAP.keySet();
        for (String s : keySet) {
            termsBuilders.add(AggregationBuilders.terms(s).field(s));
        }

        final int size = termsBuilders.size();
        final TermsBuilder termsBuilder = termsBuilders.get(size - 1);

        termsBuilder
                .subAggregation(
                        AggregationBuilders.stats(LATENCY_MSEC_FIELD_NAME).field(LATENCY_MSEC_FIELD_NAME)
                )
                .subAggregation(
                        AggregationBuilders.filter(RESPONSE_FIELD_NAME).filter(FilterBuilders.termFilter("_ret_code.probe_st", "noresponse"))
                );

        for (int i = size - 1; i > 0; i--) {
            termsBuilders.get(i - 1).subAggregation(termsBuilders.get(i));
        }

        BUILDERS = termsBuilders.get(0);
    }

    /**
     * 查询某时间段内数据
     *
     * @param from 查询起始时间(ms)
     * @param to   查询结束时间(ms)
     * @return 查询结果
     */
    public List<SearchVo> search(long from, long to) {

        //拼接索引名称
        final SystemProperties systemProperties = this.coreService.getSystemProperties();
        final String indexName = systemProperties.getElasticsearchIndexPrefix() + SIMPLE_DATE_FORMAT.format(new Date(from));
        final String indexType = systemProperties.getElasticsearchIndexType();

        final List<Topo> topoStatistics = TopoService.getTopoStatistics();

        final List<SearchVo> data = Lists.newArrayList();
        for (Topo topoStatistic : topoStatistics) {
            final String id = topoStatistic.getId();
            final String name = topoStatistic.getName();
            final List<Node> nodes = topoStatistic.getNodeList();

            for (Node node : nodes) {
                SearchVo searchVo = new SearchVo(from, to, id, name, node);
                List<LinkedNode> linkedNodes = getLinkedNodes(searchVo, indexType, indexName);
                searchVo.setData(linkedNodes);
                data.add(searchVo);
            }
        }
        return data;
    }

    /**
     * 查询某节点下数据
     *
     * @param searchVo  searchVo
     * @param indexName 索引名称
     * @return 查询结果
     */
    private List<LinkedNode> getLinkedNodes(SearchVo searchVo, String indexType, String indexName) {

        final SearchRequestBuilder searchRequestBuilder = ESClient.getClient().
                prepareSearch(indexName).
                setTypes(indexType).
                setSize(0).
                addAggregation(BUILDERS);

        List<FilterBuilder> mustFilterList = getFilterBuilders(searchVo);

        searchRequestBuilder.setQuery(QueryBuilders.filteredQuery(
                QueryBuilders.matchAllQuery(),
                FilterBuilders.andFilter(mustFilterList.toArray(new FilterBuilder[mustFilterList.size()])))
        );

        final ListenableActionFuture<SearchResponse> execute = searchRequestBuilder.execute();
        final SearchResponse searchResponse = execute.actionGet();

        final Aggregations aggregations = searchResponse.getAggregations();
        //MessageGatewayHelper.debugSearchResponse(searchRequestBuilder, searchResponse);

        final ArrayList<String> groupIds = Lists.newArrayList(STATISTICS_GROUP_FIELD_MAP.keySet());
        final Map<String, Aggregation> aggregationMap = aggregations.asMap();

        List<LinkedNode> linkedNodes = Lists.newLinkedList();

        final String groupKey = groupIds.get(0);
        final Aggregation aggregation = aggregationMap.remove(groupKey);
        final Collection<Terms.Bucket> buckets = ((InternalTerms) aggregation).getBuckets();
        for (Terms.Bucket bucket : buckets) {
            LinkedNode node = new LinkedNode(groupKey, bucket.getKey());
            linkedNodes.add(node);
            analyzerAggregations(aggregationMap.size() == 0 ? bucket.getAggregations().asMap() : aggregationMap, node);
        }
        return linkedNodes;
    }

    /**
     * 拼接查询过滤器
     *
     * @param searchVo searchVo
     * @return 查询过滤器
     */
    private List<FilterBuilder> getFilterBuilders(SearchVo searchVo) {

        long from = searchVo.getStartTime() / 1000L;
        long to = searchVo.getEndTime() / 1000L;
        List<FilterBuilder> mustFilterList = Lists.newArrayList();
        RangeFilterBuilder rangeFilterBuilder = FilterBuilders.rangeFilter(TIME_FIELD_NAME);
        rangeFilterBuilder.gte(from).includeLower(true);
        rangeFilterBuilder.lt(to).includeUpper(false);

        mustFilterList.add(rangeFilterBuilder);

        RangeFilterBuilder smallRangeFb = FilterBuilders.rangeFilter(TIME_FIELD_NAME);
        smallRangeFb.gte(from).includeLower(true);
        smallRangeFb.lt(to).includeUpper(false);
        smallRangeFb.cache(false);
        mustFilterList.add(smallRangeFb);


        final TermFilterBuilder termFilterBuilder = FilterBuilders.termFilter(STREAMS_FIELD_NAME, searchVo.getNode().getStatisticsStreamIds());
        mustFilterList.add(termFilterBuilder);

        return mustFilterList;
    }

    /**
     * 解析 Aggregation 数据
     *
     * @param aggregations aggregations
     * @param linkedNode   链表封装数据
     */
    private void analyzerAggregations(Map<String, Aggregation> aggregations, LinkedNode linkedNode) {

        final Collection<Aggregation> values = aggregations.values();

        if (aggregations.isEmpty()) {
            return;
        }

        for (Aggregation value : values) {
            final String name = value.getName();

            if (value instanceof InternalTerms) {
                final Collection<Terms.Bucket> buckets = ((InternalTerms) value).getBuckets();
                for (Terms.Bucket bucket : buckets) {
                    final LinkedNode node = new LinkedNode(name, bucket.getKey());
                    linkedNode.setNext(node);
                    analyzerAggregations(bucket.getAggregations().asMap(), node);
                }
            } else if (value instanceof InternalStats) {
                final InternalStats internalStats = (InternalStats) value;
                final long count = internalStats.getCount();
                final double avg = internalStats.getAvg();

                final LinkedNode countNode = new LinkedNode(COUNT_FIELD_NAME, count);
                final LinkedNode latencyMsecNode = new LinkedNode(LATENCY_MSEC_FIELD_NAME, NUMBER_FORMAT.format(avg));
                countNode.setNext(latencyMsecNode);
                linkedNode.setNext(countNode);
            } else if (value instanceof InternalFilter) {
                final InternalFilter internalFilter = (InternalFilter) value;
                final long docCount = internalFilter.getDocCount();
                final LinkedNode responseNode = new LinkedNode(RESPONSE_FIELD_NAME, docCount);
                linkedNode.setNext(responseNode);
            }
        }
    }
}
