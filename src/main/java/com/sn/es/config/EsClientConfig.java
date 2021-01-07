package com.sn.es.config;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by nings on 2020/12/8.
 *
 * http://www.mamicode.com/info-detail-3062664.html
 */
@Slf4j
public class EsClientConfig /*implements InitializingBean*/{

    // 低水平客户端,高水平客户端还不完善,但官方推荐用高水平
    //privat static RestClient restClient;
    private static RestHighLevelClient restHighLevelClient;


    /**
     * 初始化
     * @throws Exception
     */
    /*@Override
    public void afterPropertiesSet() throws Exception {
        *//*restHighLevelClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost(host,port, scheme))
                //RestClient.builder(new HttpHost("host",0,"http"),new HttpHost("host",0,"http"))
        );*//*
        restHighLevelClient = ElasticSearchPoolUtil.getClient();
        //restClient = restHighLevelClient.getLowLevelClient();
    }*/



    /**
     * 查询符合条件的数据(单条件查询)
     * @param indexme
    * @param type
     */
    @SuppressWarnings("Duplicates")
    public static List<Map<String,Object>> searchAllByOne(String indexme, String type, String filed, String keyword, int start, int count){
        SearchRequest searchRequest = new SearchRequest(indexme);
        SearchSourceBuilder builder = new SearchSourceBuilder();
        MatchQueryBuilder queryBuilder = null;
        if (filed != null && filed.length()>0){
            queryBuilder = QueryBuilders.matchQuery(filed,keyword);
            builder.query(queryBuilder);
        }
        builder.from(start);
        builder.size(count);
        builder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
        searchRequest.source(builder);
        if (type!=null){
            searchRequest.types(type);
        }
        try {
            restHighLevelClient = ElasticSearchPoolUtil.getClient();
            SearchResponse response = restHighLevelClient.search(searchRequest,RequestOptions.DEFAULT);
            SearchHits searchHits = response.getHits();// 命中的数据数
            List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
            SearchHit[] hits = searchHits.getHits();
            for (SearchHit hit:hits){
                list.add(hit.getSourceAsMap());
            }
            return list;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            ElasticSearchPoolUtil.returnClient(restHighLevelClient);
        }

    }

    /**
     * 多个字段匹配一个值
     * @param indexme
     * @param type
     * @param filed
     * @param keyword
     * @param filed2
     * @param start
     * @param count
     * @return
     */
    @SuppressWarnings("Duplicates")
    public static List<Map<String,Object>> searchAllByMulti(String indexme, String type, String filed, String keyword,
                                                            String filed2, int start, int count){
        SearchRequest searchRequest = new SearchRequest(indexme);
        SearchSourceBuilder builder = new SearchSourceBuilder();
        MultiMatchQueryBuilder mqueryBuilder = null;
        if (filed != null && filed.length()>0){
            mqueryBuilder = QueryBuilders.multiMatchQuery(keyword,filed,filed2);
            // ****
            // 字段指定分词器进行查询,这里使用ik分词器，
            // ik_max_word:
            //会将文本做最细粒度的拆分，比如会将“中华人民共和国国歌”拆分为“中华人民共和国,中华人民,中华,华人,人民共和国,人民,人,民,共和国,
            //共和,和,国国,国歌”，会穷尽各种可能的组合；
            //ik_smart: 会做最粗粒度的拆分，比如会将“中华人民共和国国歌”拆分为“中华人民共和国,国歌”。
            // ****
            mqueryBuilder.analyzer("ik_smart"); // 智能分词
            builder.query(mqueryBuilder);
        }
        builder.from(start);
        builder.size(count);
        builder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
        searchRequest.source(builder);
        if (type!=null){
            searchRequest.types(type);
        }
        try {
            restHighLevelClient = ElasticSearchPoolUtil.getClient();
            SearchResponse response = restHighLevelClient.search(searchRequest,RequestOptions.DEFAULT);
            SearchHits searchHits = response.getHits();// 命中的数据数
            List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
            SearchHit[] hits = searchHits.getHits();
            for (SearchHit hit:hits){
                list.add(hit.getSourceAsMap());
            }
            return list;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            ElasticSearchPoolUtil.returnClient(restHighLevelClient);
        }

    }

    @SuppressWarnings("Duplicates")
    public static List<Map<String,Object>> searchAll(String indexme, String type, int start, int count){
        SearchRequest searchRequest = new SearchRequest(indexme);
        SearchSourceBuilder builder = new SearchSourceBuilder();
        // 查询所有type中的数据
        builder.query(QueryBuilders.matchAllQuery());
        builder.from(start);
        builder.size(count);
        builder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
        builder.timeout(new TimeValue(60, TimeUnit.SECONDS)); // 设置超时时间
        // 设置高亮显示
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("name"); // 设置高亮显示的字段名称
        highlightBuilder.field("content"); // 设置高亮显示的字段名称
        highlightBuilder.requireFieldMatch(false); // 如果要多个字段高亮,这项要为false
        highlightBuilder.preTags("<span style='color:red'>"); // 设置颜色
        highlightBuilder.postTags("</span>");

        // 下面这两项,如果你要高亮如文字内容等有很多字的字段,必须配置,不然会导致高亮不全,文章内容缺失等
        highlightBuilder.fragmentSize(800000); // 最大高亮分片数
        highlightBuilder.numOfFragments(0); // 从第一个分片获取高亮片段
        builder.highlighter(highlightBuilder);

        searchRequest.source(builder);
        if (type!=null) {
            searchRequest.types(type);
        }
        // 设置偏好参数，如设置搜索本地分片的偏好，默认是在分片中随机检索
        searchRequest.preference("_local");

        // ignore_unavailable ：是否忽略不可用的索引
        // allow_no_indices：是否允许索引不存在
        // expandToOpenIndices ：通配符表达式将扩展为打开的索引
        // expandToClosedIndices ：通配符表达式将扩展为关闭的索引
        //searchRequest.indicesOptions(IndicesOptions.fromOptions(true,true,true,false));
        try {
            restHighLevelClient = ElasticSearchPoolUtil.getClient();
            SearchResponse response = restHighLevelClient.search(searchRequest,RequestOptions.DEFAULT);
            SearchHits searchHits = response.getHits();// 命中的数据数
            List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
            SearchHit[] hits = searchHits.getHits();
            for (SearchHit hit:hits){
                list.add(hit.getSourceAsMap());
            }
            return list;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            ElasticSearchPoolUtil.returnClient(restHighLevelClient);
        }

    }

    /**
     * 模糊查询 fuzzy
     * @param indexme
     * @param type
     * @param filed
     * @param keyword
     * @param start
     * @param count
     * @return
     */
    @SuppressWarnings("Duplicates")
    public static List<Map<String,Object>> searchAllByFuzzy(String indexme, String type, String filed, String keyword,
                                                            int start, int count){
        SearchRequest searchRequest = new SearchRequest(indexme);
        SearchSourceBuilder builder = new SearchSourceBuilder();
        FuzzyQueryBuilder fuzzy = null;// 这是真正的模糊查询
        //WildcardQueryBuilder wild = null;  // 这个指的是通配符，完成的模糊查询，需要我们自己指定
        if (filed != null && filed.length()>0){
            fuzzy = QueryBuilders.fuzzyQuery(filed,keyword);
            //wild = new WildcardQueryBuilder(filed,keyword);
            builder.query(fuzzy);
            //builder.query(wild);
        }
        builder.from(start);
        builder.size(count);
        builder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
        searchRequest.source(builder);
        if (type!=null){
            searchRequest.types(type);
        }
        try {
            restHighLevelClient = ElasticSearchPoolUtil.getClient();
            SearchResponse response = restHighLevelClient.search(searchRequest);
            SearchHits searchHits = response.getHits();// 命中的数据数
            List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
            SearchHit[] hits = searchHits.getHits();
            for (SearchHit hit:hits){
                list.add(hit.getSourceAsMap());
            }
            return list;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            ElasticSearchPoolUtil.returnClient(restHighLevelClient);
        }

    }

    /**
     * 在使用matchQuery等时，在执行查询时，搜索的词会被分词器分词，而使用matchPhraseQuery时，
     * 不会被分词器分词，而是直接以一个短语的形式查询，而如果你在创建索引所使用的field的value中没有这么一个短语（顺序无差，且连接在一起），
     * 那么将查询不出任何结果
     * @param indexme
     * @param type
     * @param filed
     * @param keyword
     * @param start
     * @param count
     * @return
     */
    @SuppressWarnings("Duplicates")
    public static List<Map<String,Object>> searchAllByMatchPhrase(String indexme, String type, String filed, String keyword,
                                                            int start, int count){
        SearchRequest searchRequest = new SearchRequest(indexme);
        SearchSourceBuilder builder = new SearchSourceBuilder();
        MatchPhraseQueryBuilder phrase = null;
        if (filed != null && filed.length()>0){
            phrase = QueryBuilders.matchPhraseQuery(filed,keyword);
            builder.query(phrase);
        }
        builder.from(start);
        builder.size(count);
        builder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
        searchRequest.source(builder);
        if (type!=null){
            searchRequest.types(type);
        }
        try {
            restHighLevelClient = ElasticSearchPoolUtil.getClient();
            SearchResponse response = restHighLevelClient.search(searchRequest);
            SearchHits searchHits = response.getHits();// 命中的数据数
            List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
            SearchHit[] hits = searchHits.getHits();
            for (SearchHit hit:hits){
                list.add(hit.getSourceAsMap());
            }
            return list;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            ElasticSearchPoolUtil.returnClient(restHighLevelClient);
        }

    }

    /**
     * 简单查询term(s)
     * @param indexme
     * @param type
     * @param filed
     * @param keyword
     * @param keyword2
     * @param start
     * @param count
     * @return
     */
    @SuppressWarnings("Duplicates")
    public static List<Map<String,Object>> searchAllByTerm(String indexme, String type, String filed, String keyword,
                                                           String keyword2, int start, int count){
        SearchRequest searchRequest = new SearchRequest(indexme);
        SearchSourceBuilder builder = new SearchSourceBuilder();
        TermQueryBuilder term = null;
        //TermsQueryBuilder terms = null;
        if (filed != null && filed.length()>0){
            term = QueryBuilders.termQuery(filed,keyword);
            //terms = new TermsQueryBuilder(filed,keyword,keyword2); // 字段匹配多个词条，满足一个即可
            // 单词条查询，最简单的查询，不会进行任何处理，不会分词，但是会将查询内容转换成小写；MatchQuery会进行分词，此过程已自动转换成小写
            builder.query(term);
        }
        builder.from(start);
        builder.size(count);
        builder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
        searchRequest.source(builder);
        if (type!=null){
            searchRequest.types(type);
        }
        try {
            restHighLevelClient = ElasticSearchPoolUtil.getClient();
            SearchResponse response = restHighLevelClient.search(searchRequest,RequestOptions.DEFAULT);
            SearchHits searchHits = response.getHits();// 命中的数据数
            List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
            SearchHit[] hits = searchHits.getHits();
            for (SearchHit hit:hits){
                list.add(hit.getSourceAsMap());
            }
            return list;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            ElasticSearchPoolUtil.returnClient(restHighLevelClient);
        }

    }

    /**
     * 范围查询
     * @param indexme
     * @param type
     * @param filed
     * @param from
     * @param to
     * @param start
     * @param count
     * @return
     */
    @SuppressWarnings("Duplicates")
    public static List<Map<String,Object>> searchAllByRange(String indexme, String type, String filed,
                                                            int from, int to, int start, int count){
        SearchRequest searchRequest = new SearchRequest(indexme);
        SearchSourceBuilder builder = new SearchSourceBuilder();
        RangeQueryBuilder range = QueryBuilders.rangeQuery(filed).from(from).to(to).includeLower(true).includeUpper(false).boost(1.0f);
        // 可以进行设置权重，重要性大的，可以设置，提高匹配概率，默认为 1，大于1，概率增大
        if (filed != null && filed.length()>0){
            builder.query(range);
        }
        builder.from(start);
        builder.size(count);
        builder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
        searchRequest.source(builder);
        if (type!=null){
            searchRequest.types(type);
        }
        try {
            restHighLevelClient = ElasticSearchPoolUtil.getClient();
            SearchResponse response = restHighLevelClient.search(searchRequest);
            SearchHits searchHits = response.getHits();// 命中的数据数
            List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
            SearchHit[] hits = searchHits.getHits();
            for (SearchHit hit:hits){
                list.add(hit.getSourceAsMap());
            }
            return list;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            ElasticSearchPoolUtil.returnClient(restHighLevelClient);
        }

    }


    @SuppressWarnings("Duplicates")
    public static List<Map<String,Object>> searchAllByBool(String indexme, String type, String filed, String keyword,
                                                           int from, int to, int start, int count){
        SearchRequest searchRequest = new SearchRequest(indexme);
        SearchSourceBuilder builder = new SearchSourceBuilder();

        BoolQueryBuilder bool = QueryBuilders.boolQuery();
        // 使用filter 效率比 must高，因为filter只进行条件过滤，不进行计分
        RangeQueryBuilder range = QueryBuilders.rangeQuery(filed).from(from).to(to).includeLower(true).includeUpper(false).boost(1.0f);
        TermQueryBuilder term = QueryBuilders.termQuery(filed,keyword);
        // 可以进行设置权重，重要性大的，可以设置，提高匹配概率，默认为 1，大于1，概率增大
        bool.filter(range);
        bool.filter(term);
        //bool.must(range);
        //bool.must(term);
        builder.query(bool); // 多条件查询

        builder.from(start);
        builder.size(count);
        builder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
        searchRequest.source(builder);
        if (type!=null){
            searchRequest.types(type);
        }
        try {
            restHighLevelClient = ElasticSearchPoolUtil.getClient();
            SearchResponse response = restHighLevelClient.search(searchRequest);
            SearchHits searchHits = response.getHits();// 命中的数据数
            List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
            SearchHit[] hits = searchHits.getHits();
            for (SearchHit hit:hits){
                list.add(hit.getSourceAsMap());
            }
            return list;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            ElasticSearchPoolUtil.returnClient(restHighLevelClient);
        }

    }

    /**************************************上面是查询********************************************/

    /**
     * 删除，没有找到条件
     * @param indexme
     * @param type
     * @param id
     * @return
     */
    @SuppressWarnings("Duplicates")
    public static boolean deleteById(String indexme, String type, String id){
        // 根据id删除
        DeleteRequest deleteRequest = new DeleteRequest(indexme);
        deleteRequest.id(id);
        if (type!=null){
            deleteRequest.type(type);
        }
        try {
            restHighLevelClient = ElasticSearchPoolUtil.getClient();
            DeleteResponse response = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
            DocWriteResponse.Result result = response.getResult();
            if (result==DocWriteResponse.Result.DELETED){  // 结果已经删除
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ElasticSearchPoolUtil.returnClient(restHighLevelClient);
        }
        return false;
    }

    /**
     * 根据指定条件删除
     * @param indexme
     * @param type
     * @param map
     * @param size
     * @return
     */
    @SuppressWarnings("Duplicates")
    public static boolean deleteByCondition(String indexme, String type, Map<String,Object> map,int size){
        DeleteByQueryRequest request = new DeleteByQueryRequest(indexme);
        if (type!=null){
            request.types(type);  // 指定类型
        }

        BoolQueryBuilder bool = QueryBuilders.boolQuery();
        for (Map.Entry<String,Object> entry : map.entrySet()){
            bool.must(QueryBuilders.matchQuery(entry.getKey(),entry.getValue()));
        }
        request.setQuery(bool);
        request.setSize(size);
        try {
            restHighLevelClient = ElasticSearchPoolUtil.getClient();
            BulkByScrollResponse response = restHighLevelClient.deleteByQuery(request,RequestOptions.DEFAULT);
            long deleted = response.getDeleted(); // 删除条数
            if (deleted>0){
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ElasticSearchPoolUtil.returnClient(restHighLevelClient);
        }
        return false;
    }

    /*****************************************上面是删除********************************************/
    /**
     * 新增index（索引库）
     * @param indexme
     * @return
     */
    public static boolean createIndex(String indexme){
        CreateIndexRequest indexRequest = new CreateIndexRequest(indexme);
        try {
            restHighLevelClient = ElasticSearchPoolUtil.getClient();
            restHighLevelClient.indices().createAsync(indexRequest, RequestOptions.DEFAULT, new ActionListener<CreateIndexResponse>() {
                @Override
                public void onResponse(CreateIndexResponse createIndexResponse) {
                    log.info("创建索引index返回："+createIndexResponse);
                }
                @Override
                public void onFailure(Exception e) {
                    log.error("创建index报错："+e.getMessage());
                }
            });
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ElasticSearchPoolUtil.returnClient(restHighLevelClient);
        }
        return false;
    }

    /**
     * 判断索引是否存在
     * @param indexName
     * @return
     */
    public static boolean checkIndexExist(String indexName){
        GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
        try {
            restHighLevelClient = ElasticSearchPoolUtil.getClient();
            boolean flag = restHighLevelClient.indices().exists(getIndexRequest,RequestOptions.DEFAULT);
            return flag;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ElasticSearchPoolUtil.returnClient(restHighLevelClient);
        }
        return false;
    }

    /**
     * 删除索引
     * @param indexName
     * @return
     */
    public static boolean deleteIndex(String indexName){
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);
        try {
            restHighLevelClient = ElasticSearchPoolUtil.getClient();
            AcknowledgedResponse delete = restHighLevelClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
            boolean acknowledged = delete.isAcknowledged();
            return acknowledged;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ElasticSearchPoolUtil.returnClient(restHighLevelClient);
        }
        return false;
    }

    public static boolean checkTextExist(String indexName,String type,String id){
        GetRequest getRequest = new GetRequest(indexName,type,id);
        // 不获取返回的 _source 的上下文了
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        // 获取上下文
        //getRequest.fetchSourceContext(new FetchSourceContext(true));

        /**
         *GET twitter/_doc/1?stored_fields=tags,counter
         *设置索引的时候，我们给某些字段的store属性设置为true，
         * 在查询时，请求中可以携带stored_fields参数，指定某些字段，
         * 最后，这些字段会被包含在返回的结果中。如果请求中携带的字段没有被储存，将会被忽略。
         *
         * 这里查询不携带stored_fields字段
         */
        getRequest.storedFields("_none_");
        try {
            restHighLevelClient = ElasticSearchPoolUtil.getClient();
            boolean exists = restHighLevelClient.exists(getRequest, RequestOptions.DEFAULT);
            return exists;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ElasticSearchPoolUtil.returnClient(restHighLevelClient);
        }
        return false;
    }

    /**
     * 获取文档内容
     * @param indexName
     * @param type
     * @param id
     * @return
     */
    public static String getTextContent(String indexName,String type,String id){
        GetRequest getRequest = new GetRequest(indexName);
        getRequest.id(id);
        if (type!=null){
            getRequest.type(type);
        }
        try {
            restHighLevelClient = ElasticSearchPoolUtil.getClient();
            GetResponse documentFields = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
            return documentFields.getSourceAsString();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ElasticSearchPoolUtil.returnClient(restHighLevelClient);
        }
        return "";
    }

    /**
     * 更新文档内容
     * @param indexName
     * @param type
     * @param id
     * @return
     */
    public static boolean updateTextContent(String indexName,String type,String id){
        UpdateRequest updateRequest = new UpdateRequest(indexName,type,id);
        updateRequest.timeout("1s");  // 设置超时时间1s
        Map<String,Object> map = new HashMap<String,Object>();
        map.put("name","suning");
        updateRequest.doc(JSON.toJSON(map), XContentType.JSON); // 赋值
        try {
            restHighLevelClient = ElasticSearchPoolUtil.getClient();
            UpdateResponse update = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
            return update.getResult().equals(DocWriteResponse.Result.UPDATED);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ElasticSearchPoolUtil.returnClient(restHighLevelClient);
        }
        return false;
    }

    /**
     * 删除文档
     * @param indexName
     * @param type
     * @param id
     * @return
     */
    public static boolean deleteTextContent(String indexName,String type,String id){
        DeleteRequest deleteRequest = new DeleteRequest(indexName);
        deleteRequest.id(id);
        if (type!=null){
            deleteRequest.type(type);
        }
        try {
            restHighLevelClient = ElasticSearchPoolUtil.getClient();
            DeleteResponse delete = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
            return delete.getResult().equals(DocWriteResponse.Result.DELETED);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ElasticSearchPoolUtil.returnClient(restHighLevelClient);
        }
        return false;
    }

    /**
     * 新增文档
     * BulkRequest  可以进行大批量的数据的删除，新增，更新操作，bulk就是大量的意思
     * @param index
     * @param type
     * @return
     */
    public static boolean addTextContent(String index,String type,List<Map<String,Object>> list){
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("10s");  // 设置超时时间，默认是不开启超时的
        int i = 0;
        for (Map<String,Object> map:list){
            i++;
            bulkRequest.add(new IndexRequest().id(i+"").index(index).type(type).routing("su_es_rt").source(map),XContentType.JSON);
        }
        try {
            restHighLevelClient = ElasticSearchPoolUtil.getClient();
            BulkResponse bulk = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            return !bulk.hasFailures();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ElasticSearchPoolUtil.returnClient(restHighLevelClient);
        }
        return false;
    }

}
