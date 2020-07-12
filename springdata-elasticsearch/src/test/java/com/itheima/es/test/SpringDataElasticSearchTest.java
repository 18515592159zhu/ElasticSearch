package com.itheima.es.test;

import com.itheima.es.dao.ArticleRepository;
import com.itheima.es.entity.Article;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;
import java.util.Optional;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext.xml")
public class SpringDataElasticSearchTest {

    @Autowired
    private ArticleRepository articleRepository;
    @Autowired
    private ElasticsearchTemplate template;

    /**
     * 创建索引和映射
     */
    @Test
    public void createIndex() throws Exception {
        // 创建索引，并配置映射关系
        template.createIndex(Article.class);
        // 配置映射关系
        // template.putMapping(Article.class);
    }

    /**
     * 测试添加文档
     */
    @Test
    public void addDocument() throws Exception {
        Article article = new Article();
        article.setId(100);
        article.setTitle("测试SpringData ElasticSearch");
        article.setContent("Spring Data ElasticSearch 基于 spring data API 简化 elasticSearch操作，将原始操作elasticSearch的客户端API进行封装Spring Data为Elasticsearch Elasticsearch项目提供集成搜索引擎");
        articleRepository.save(article);
    }

    /**
     * 测试保存
     */
    @Test
    public void save() {
        Article article = new Article();
        article.setId(1001);
        article.setTitle("elasticSearch 3.0版本发布");
        article.setContent("ElasticSearch是一个基于Lucene的搜索服务器。它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口");
        articleRepository.save(article);
    }

    /**
     * 测试更新
     */
    @Test
    public void update() {
        Article article = new Article();
        article.setId(1001);
        article.setTitle("elasticSearch 3.0版本发布...更新");
        article.setContent("ElasticSearch是一个基于Lucene的搜索服务器。它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口");
        articleRepository.save(article);
    }

    /**
     * 测试删除
     */
    @Test
    public void deleteDocumentById() throws Exception {
        articleRepository.deleteById(1001l);
        //全部删除
        //articleRepository.deleteAll();
    }

    /**
     * 批量插入100条数据
     */
    @Test
    public void save100() {
        for (int i = 1; i <= 99; i++) {
            Article article = new Article();
            article.setId(i);
            article.setTitle(i + "elasticSearch 3.0版本发布..，更新");
            article.setContent(i + "ElasticSearch是一个基于Lucene的搜索服务器。它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口");
            articleRepository.save(article);
        }
    }


    @Test
    public void findAll() throws Exception {
        Iterable<Article> articles = articleRepository.findAll();
        articles.forEach(a -> System.out.println(a));
    }

    @Test
    public void testFindById() throws Exception {
        Optional<Article> optional = articleRepository.findById(1l);
        Article article = optional.get();
        System.out.println(article);
    }

    @Test
    public void testFindByTitle() throws Exception {
        List<Article> list = articleRepository.findByTitle("新版本发布了");
        list.stream().forEach(a -> System.out.println(a));
    }

    @Test
    public void testFindByTitleOrContent() throws Exception {
        Pageable pageable = PageRequest.of(1, 15);
        articleRepository.findByTitleOrContent("版本", "搜素服务器", pageable)
                .forEach(a -> System.out.println(a));
    }

    @Test
    public void testNativeSearchQuery() throws Exception {
        //创建一个查询对象
        NativeSearchQuery query = new NativeSearchQueryBuilder()
                // 设置查询条件，此处可以使用QueryBuilders创建多种查询
                .withQuery(QueryBuilders.queryStringQuery("新版本发布了").defaultField("title"))
                // 还可以设置分页信息
                .withPageable(PageRequest.of(0, 15))
                .build();
        //执行查询
        List<Article> articleList = template.queryForList(query, Article.class);
        articleList.forEach(a -> System.out.println(a));
    }
}