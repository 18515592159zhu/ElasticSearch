package com.itheima.es;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;

public class ElasticSearchClientTest {

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
     * 创建索引
     */
    @Test
    public void testCreateIndex() throws Exception {

        // 1、创建一个Settings对象，相当于是一个配置信息。主要配置集群的名称。
        Settings settings = Settings.builder().put("cluster.name", "my-elasticsearch").build();

        // 2、创建一个客户端Client对象
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9303));

        // 3、使用client对象创建一个索引库，名称为 blog2
        client.admin().indices().prepareCreate("blog2").get();

        // 4、关闭client对象
        client.close();
    }

    /**
     * 创建映射
     */
    @Test
    public void testSetMappings() throws Exception {

        // 1、创建一个Settings对象，相当于是一个配置信息。主要配置集群的名称。
        Settings settings = Settings.builder().put("cluster.name", "my-elasticsearch").build();

        // 2、创建一个客户端Client对象
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9303));

        //创建一个Mappings信息,添加映射

        /*
            {
                "article": {
                    "properties": {
                        "id": {
                            "type": "long",
                            "store": true
                        },
                        "title": {
                            "type": "text",
                            "store": true,
                            "index": true,
                            "analyzer": "ik_smart"
                        },
                        "content": {
                            "type": "text",
                            "store": true,
                            "index": true,
                            "analyzer": "ik_smart"
                        }
                    }
                }
            }
         */
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("article")
                .startObject("properties")
                .startObject("id")
                .field("type", "long")
                .field("store", true)
                .endObject()
                .startObject("title")
                .field("type", "text")
                .field("store", true)
                .field("analyzer", "ik_smart")
                .endObject()
                .startObject("content")
                .field("type", "text")
                .field("store", true)
                .field("analyzer", "ik_smart")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        // 使用client把mapping信息设置到索引库中
        client.admin().indices()
                // 设置要做映射的索引
                .preparePutMapping("blog2")
                // 设置要做映射的type
                .setType("article")
                // mapping信息，可以是XContentBuilder对象可以是json格式的字符串
                .setSource(builder)
                // 执行操作
                .get();
        // 关闭链接
        client.close();
    }

    /**
     * 创建文档(通过XContentBuilder)
     */
    @Test
    public void testAddDocument() throws Exception {
        // 创建一个client对象
        // 创建一个文档对象
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("id", 1l)
                .field("title", "ElasticSearch是一个基于Lucene的搜索服务器")
                .field("content", "它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。Elasticsearch是用\n" +
                        "Java开发的，并作为Apache许可条款下的开放源码发布，是当前流行的企业级搜索引擎。设计用于云计算中，能够达到\n" +
                        "实时搜索，稳定，可靠，快速，安装使用方便。")
                .endObject();

        // 把文档对象添加到索引库
        client.prepareIndex()
                // 设置索引名称
                .setIndex("blog2")
                // 设置type
                .setType("article")
                // 设置文档的id，如果不设置的话自动的生成一个id
                .setId("1")
                // 设置文档信息
                .setSource(builder)
                //执行操作
                .get();
        // 关闭客户端
        client.close();
    }

    /**
     * 创建文档(通过实体转json)
     */
    @Test
    public void testAddDocument2() throws Exception {
        // 创建一个Article对象
        Article article = new Article();

        // 设置对象的属性
        // {id:xxx, title:xxx, content:xxx}
        article.setId(2l);
        article.setTitle("搜索工作其实很快乐");
        article.setContent("我们希望我们的搜索解决方案要快，我们希望有一个零配置和一个完全免费的搜索模式，\n" +
                "我们希望能够简单地使用JSON通过HTTP的索引数据，我们希望我们的搜索服务器始终可用，我们希望能够一台开始并扩\n" +
                "展到数百，我们要实时搜索，我们要简单的多租户，我们希望建立一个云的解决方案。Elasticsearch旨在解决所有这\n" +
                "些问题和更多的问题。");

        // 把article对象转换成json格式的字符串
        ObjectMapper objectMapper = new ObjectMapper();
        String jsonDocument = objectMapper.writeValueAsString(article);
        System.out.println(jsonDocument);

        // 使用client对象把文档写入索引库
        // id为String类型的话，可以通过article.getId().toString()设置
        client.prepareIndex("blog2", "article", "2")
                .setSource(jsonDocument, XContentType.JSON)
                .get();
        //关闭客户端
        client.close();
    }

    /**
     * 批量插入100条数据
     */
    @Test
    public void testAddDocument3() throws Exception {

        ObjectMapper objectMapper = new ObjectMapper();

        for (int i = 3; i <= 100; i++) {
            // 创建一个Article对象
            Article article = new Article();

            // 描述json 数据,设置对象的属性
            article.setId(i);
            article.setTitle("搜索工作其实很快乐" + i);
            article.setContent("我们希望我们的搜索解决方案要快，我们希望有一个零配置和一个完全免费的搜索模式，\n" +
                    "我们希望能够简单地使用JSON通过HTTP的索引数据，我们希望我们的搜索服务器始终可用，我们希望能够一台开始并扩\n" +
                    "展到数百，我们要实时搜索，我们要简单的多租户，我们希望建立一个云的解决方案。Elasticsearch旨在解决所有这\n" +
                    "些问题和更多的问题。" + i);

            // 把article对象转换成json格式的字符串。
            String jsonDocument = objectMapper.writeValueAsString(article);
            System.out.println(jsonDocument);
            // 使用client对象把文档写入索引库
            client.prepareIndex("blog2", "article", i + "")
                    .setSource(jsonDocument, XContentType.JSON)
                    .get();
        }
        //释放资源
        client.close();
    }
}