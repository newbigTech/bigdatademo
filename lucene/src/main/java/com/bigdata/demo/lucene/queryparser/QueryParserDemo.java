package com.bigdata.demo.lucene.queryparser;

import com.bigdata.demo.lucene.analyzer.ik.IKAnalyzer4Lucene7;
import com.bigdata.demo.lucene.common.Common;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;

public class QueryParserDemo {

    public static void main(String[] args) throws IOException, ParseException {
        Analyzer analyzer = new IKAnalyzer4Lucene7(true);

        Directory directory = FSDirectory.open(new File(Common.LUCENE_DB_DIR).toPath());
        DirectoryReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);

        // 查询解析器, new QueryParser(默认查询字段, 分词器);
        QueryParser parser = new QueryParser("name", analyzer);

        System.out.println("****************** 不使用默认字段查询 ******************");
        // 查询 : +(name:"联想 笔记本电脑" simpleIntro:英特尔) +type:电脑
        String lql = "(name:\"联想笔记本电脑\" OR simpleIntro:英特尔) AND type:电脑";
        Query query = parser.parse(lql);
        Common.doQuery(query, searcher);

        System.out.println("****************** 使用默认字段查询 ******************");
        // 查询 : +(name:"联想 笔记本电脑" simpleIntro:英特尔) +name:电脑
        String lql2 = "(\"联想笔记本电脑\" OR simpleIntro:英特尔) AND 电脑";
        Query query2 = parser.parse(lql2);
        Common.doQuery(query2, searcher);
    }
}
