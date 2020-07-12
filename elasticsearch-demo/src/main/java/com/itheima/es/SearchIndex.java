package com.itheima.es;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Iterator;
import java.util.Map;

public class SearchIndex {
    private TransportClient client;

    @Before
    public void init() throws Exception {
        //创建一个Settings对象
        Settings settings = Settings.builder()
                .put("cluster.name", "my-elasticsearch")
                .build();
        //创建一个TransPortClient对象
        client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9303));

    }

    /**
     * 关键词查询
     */
    @Test
    public void testTermQuery() throws Exception {
        // 1、创建es客户端连接对象
        // 2、设置搜索条件
        SearchResponse searchResponse = client.prepareSearch("blog2")
                .setTypes("article")
                .setQuery(QueryBuilders.termQuery("content", "搜索"))
                .get();

        // 3、遍历搜索结果数据
        SearchHits hits = searchResponse.getHits(); // 获取命中次数，查询结果有多少对象
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next(); // 每个查询对象
            System.out.println(searchHit.getSourceAsString()); // 获取字符串格式打印，以json格式输出
            // 取文档属性
            System.out.println("-----------文档的属性");
            Map<String, Object> document = searchHit.getSource();
            System.out.println("id：" + document.get("id"));
            System.out.println("title：" + document.get("title"));
            System.out.println("content：" + document.get("content"));
        }

        // 4、释放资源
        client.close();
    }

    /**
     * 字符串查询
     */
    @Test
    public void estStringQuery() throws Exception {
        // 1、创建es客户端连接对象
        // 2、设置搜索条件，并执行操作
        SearchResponse searchResponse = client.prepareSearch("blog2")
                .setTypes("article")
                .setQuery(QueryBuilders.queryStringQuery("搜索")).get();
        // 3、遍历搜索结果数据
        SearchHits hits = searchResponse.getHits(); // 获取命中次数，查询结果有多少对象
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next(); // 每个查询对象
            System.out.println(searchHit.getSourceAsString()); // 获取字符串格式打印，以json格式输出
            // 取文档属性
            System.out.println("-----------文档的属性");
            Map<String, Object> document = searchHit.getSource();
            System.out.println("id：" + document.get("id"));
            System.out.println("title：" + document.get("title"));
            System.out.println("content：" + document.get("content"));
        }

        // 4、释放资源
        client.close();
    }

    /**
     * 使用文档id查询文档
     */
    @Test
    public void testIdQuery() throws Exception {
        // 创建es客户端连接对象
        // 创建一个查询对象，设置搜索条件，并执行操作
        QueryBuilder queryBuilder = QueryBuilders.idsQuery().addIds("1", "2");
        search(queryBuilder);
    }

    /**
     * 高亮查询
     */
    @Test
    public void test4() throws Exception {
        // 创建Client连接对象
        // 搜索数据
        HighlightBuilder hiBuilder = new HighlightBuilder();
        //设置高亮数据
        hiBuilder.preTags("<font style='color:red'>");
        hiBuilder.postTags("</font>");
        hiBuilder.field("title");
        SearchResponse searchResponse = client
                .prepareSearch("blog2").setTypes("article")
                .setQuery(QueryBuilders.termQuery("title", "搜索"))
                // 设置高亮信息
                .highlighter(hiBuilder)
                //获得查询结果数据
                .get();
        //获取查询结果集
        SearchHits searchHits = searchResponse.getHits();
        System.out.println("共搜到:" + searchHits.getTotalHits() + "条结果!");
        //遍历结果
        for (SearchHit hit : searchHits) {
            System.out.println("String方式打印文档搜索内容:");
            System.out.println(hit.getSourceAsString());
            System.out.println("Map方式打印高亮内容");
            System.out.println(hit.getHighlightFields());
            System.out.println("遍历高亮集合，打印高亮片段:");
            Text[] text = hit.getHighlightFields().get("title").getFragments();
            for (Text str : text) {
                System.out.println(str);
            }
        }
        //释放资源
        client.close();
    }

    /**
     * 分页查询一
     */
    @Test
    public void test1() throws Exception {
        // 创建es客户端连接对象
        // 创建一个查询对象，设置搜索条件，并执行操作
        QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();//默认每页10条记录
        search(queryBuilder);
    }

    /**
     * 分页查询二
     */
    @Test
    public void test2() throws Exception {
        // 创建Client连接对象
        // 搜索数据
        SearchResponse searchResponse = client.prepareSearch("blog2")
                .setTypes("article")
                .setQuery(QueryBuilders.matchAllQuery())//默认每页10条记录
                // 查询第2页数据，每页20条
                //setFrom()：从第几条开始检索，默认是0。
                //setSize():每页最多显示的记录数。
                .setFrom(0).setSize(5)
                .get();
        // 3、遍历搜索结果数据
        SearchHits hits = searchResponse.getHits(); // 获取命中次数，查询结果有多少对象
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next(); // 每个查询对象
            System.out.println(searchHit.getSourceAsString()); // 获取字符串格式打印，以json格式输出
            // 取文档属性
            System.out.println("-----------文档的属性");
            Map<String, Object> document = searchHit.getSource();
            System.out.println("id：" + document.get("id"));
            System.out.println("title：" + document.get("title"));
            System.out.println("content：" + document.get("content"));
        }

        // 释放资源
        client.close();
    }

    private void search(QueryBuilder queryBuilder) throws Exception {
        // 1、创建es客户端连接对象
        // 2、设置搜索条件，并执行操作
        SearchResponse searchResponse = client.prepareSearch("blog2")
                .setTypes("article")
                .setQuery(queryBuilder)
                // 默认每页10条记录
                // 查询第2页数据，每页20条
                // setFrom()：从第几条开始检索，默认是0。
                // setSize():每页最多显示的记录数。
                .setFrom(0)
                .setSize(5)
                .get();
        // 3、遍历搜索结果数据
        SearchHits hits = searchResponse.getHits(); // 获取命中次数，查询结果有多少对象
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next(); // 每个查询对象
            System.out.println(searchHit.getSourceAsString()); // 获取字符串格式打印，以json格式输出
            // 取文档属性
            System.out.println("-----------文档的属性");
            Map<String, Object> document = searchHit.getSource();
            System.out.println("id：" + document.get("id"));
            System.out.println("title：" + document.get("title"));
            System.out.println("content：" + document.get("content"));
        }
        // 关闭client
        client.close();
    }

    // 高亮查询方法提取
    private void search(QueryBuilder queryBuilder, String highlightField) throws Exception {
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        // 高亮显示的字段
        highlightBuilder.field(highlightField);
        highlightBuilder.preTags("<em>");
        highlightBuilder.postTags("</em>");
        //执行查询
        SearchResponse searchResponse = client.prepareSearch("blog2")
                .setTypes("article")
                .setQuery(queryBuilder)
                // 设置分页信息
                .setFrom(0)
                // 每页显示的行数
                .setSize(5)
                // 设置高亮信息
                .highlighter(highlightBuilder)
                .get();
        // 取查询结果
        SearchHits searchHits = searchResponse.getHits();
        // 取查询结果的总记录数
        System.out.println("查询结果总记录数：" + searchHits.getTotalHits());
        // 查询结果列表
        Iterator<SearchHit> iterator = searchHits.iterator();
        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next();
            // 打印文档对象，以json格式输出
            System.out.println(searchHit.getSourceAsString());
            // 取文档的属性
            System.out.println("-----------文档的属性");
            Map<String, Object> document = searchHit.getSource();
            System.out.println(document.get("id"));
            System.out.println(document.get("title"));
            System.out.println(document.get("content"));
            System.out.println("************高亮结果");
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            System.out.println(highlightFields);
            // 取title高亮显示的结果
            HighlightField field = highlightFields.get(highlightField);
            Text[] fragments = field.getFragments();
            if (fragments != null) {
                String title = fragments[0].toString();
                System.out.println(title);
            }
        }
        //关闭client
        client.close();
    }
}