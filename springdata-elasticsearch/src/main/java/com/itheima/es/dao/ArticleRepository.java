package com.itheima.es.dao;

import com.itheima.es.entity.Article;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public interface ArticleRepository extends ElasticsearchRepository<Article, Long> {

    List<Article> findByTitle(String title);

    List<Article> findByTitleOrContent(String title, String content);

    List<Article> findByTitleOrContent(String title, String content, Pageable pageable);
}