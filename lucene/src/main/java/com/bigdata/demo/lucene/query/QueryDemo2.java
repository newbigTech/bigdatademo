package com.bigdata.demo.lucene.query;

import com.bigdata.demo.lucene.common.Common;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;

public class QueryDemo2 {

    public static void main(String[] args) throws IOException{

        Directory directory = FSDirectory.open(new File(Common.LUCENE_DB_DIR).toPath());
        IndexReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);
        //String fieldName = "name";
        //QueryParser parser = new QueryParser(fieldName, analyzer);

        System.out.println("****************** 词项查询 ******************");
        Query query1 = new TermQuery(new Term("name", "thinkpad"));
        Common.doQuery(query1, searcher);

        System.out.println("****************** 布尔查询 ******************");
        Query query2 = new TermQuery(new Term("simpleIntro", "英特尔"));
        BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();
        booleanQueryBuilder.add(query1, BooleanClause.Occur.SHOULD); // 或
        booleanQueryBuilder.add(query2, BooleanClause.Occur.MUST); // 且
        BooleanQuery booleanQuery = booleanQueryBuilder.build();
        Common.doQuery(booleanQuery, searcher);

        System.out.println("****************** 短语查询 ******************");
        PhraseQuery phraseQuery1 = new PhraseQuery("name", "thinkpad", "carbon");
        Common.doQuery(phraseQuery1, searcher);

        System.out.println("****************** 短语查询 ******************");
        PhraseQuery phraseQuery2 = new PhraseQuery(3, "name"
                , "thinkpad", "carbon");
        Common.doQuery(phraseQuery2, searcher);

        System.out.println("****************** 多重短语查询 ******************");
        // 数组表示同一个位置匹配多个词项，使用 OR 关系匹配
        Term[] terms = new Term[2];
        terms[0] = new Term("name", "笔记本电脑");
        terms[1] = new Term("name", "笔记本");
        MultiPhraseQuery multiPhraseQuery = new MultiPhraseQuery.Builder()
                .add(new Term("name", "联想"))
                .add(terms).build();
        Common.doQuery(multiPhraseQuery, searcher);

        System.out.println("****************** 临近查询(跨度查询) ******************");
        SpanTermQuery tq1 = new SpanTermQuery(new Term("name", "thinkpad"));
        SpanTermQuery tq2 = new SpanTermQuery(new Term("name", "carbon"));
        SpanNearQuery spanNearQuery = new SpanNearQuery(new SpanTermQuery[]{tq1, tq2}
                , 1, true);
        Common.doQuery(spanNearQuery, searcher);
        // 另一种写法
        //SpanNearQuery.Builder spanNearBuilder = SpanNearQuery.newOrderedNearQuery("name")
        //        .addClause(tq1).addGap(0).setSlop(1).addClause(tq2);
        //SpanNearQuery spanNearQuery2 = spanNearBuilder.build();
        //Common.doQuery(spanNearQuery2, searcher);

        System.out.println("****************** 词项范围查询 ******************");
        TermRangeQuery termRangeQuery = TermRangeQuery.newStringRange("name"
                , "carbon", "张三", false, true);
        Common.doQuery(termRangeQuery, searcher);

        System.out.println("****************** 前缀查询 ******************");
        PrefixQuery prefixQuery = new PrefixQuery(new Term("name", "think"));
        Common.doQuery(prefixQuery, searcher);

        System.out.println("****************** 通配符查询 ******************");
        WildcardQuery wildcardQuery1 = new WildcardQuery(new Term("name", "think*"));
        Common.doQuery(wildcardQuery1, searcher);

        System.out.println("****************** 通配符查询2 ******************");
        WildcardQuery wildcardQuery2 = new WildcardQuery(new Term("name", "think????"));
        Common.doQuery(wildcardQuery2, searcher);

        System.out.println("****************** 正则查询 ******************");
        RegexpQuery regexpQuery = new RegexpQuery(new Term("name", "thin.{3}"));
        Common.doQuery(regexpQuery, searcher);

        System.out.println("****************** 模糊查询 ******************");
        // 最大可允许两个字符的不同
        FuzzyQuery fuzzyQuery = new FuzzyQuery(new Term("name","think"));
        //FuzzyQuery fuzzyQuery = new FuzzyQuery(new Term("name","thinkpdd"));
        //FuzzyQuery fuzzyQuery = new FuzzyQuery(new Term("name","thinkpaaa"));
        Common.doQuery(fuzzyQuery, searcher);

        System.out.println("****************** 数字精确查询 ******************");
        // 精确查询
        Query exactQuery = IntPoint.newExactQuery("price", 999900);
        Common.doQuery(exactQuery, searcher);

        System.out.println("****************** 数字范围查询 ******************");
        // 范围查询
        Query rangeQuery = IntPoint.newRangeQuery("price"
                , 10000000, 11000000);
        Common.doQuery(rangeQuery, searcher);

        System.out.println("****************** 数字集合查询 ******************");
        Query setQuery = IntPoint.newSetQuery("price", 10000000, 11000000, 888800);
        Common.doQuery(setQuery, searcher);
    }

}
