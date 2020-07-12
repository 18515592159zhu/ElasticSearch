package com.itheima.es.entity;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

// @Document 文档对象 （索引信息、文档类型 ）
@Document(indexName = "blog3", type = "article")
public class Article {

    // @Id 文档主键 唯一标识
    // @Field 每个文档的字段配置（类型、是否分词、是否存储、分词器 ）
    @Id
    @Field(type = FieldType.Long, store = true)
    private long id;

    @Field(type = FieldType.text, store = true, analyzer = "ik_smart")
    private String title;

    @Field(type = FieldType.text, store = true, analyzer = "ik_smart")
    private String content;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    @Override
    public String toString() {
        return "Article{" +
                "id=" + id +
                ", title='" + title + '\'' +
                ", content='" + content + '\'' +
                '}';
    }
}
