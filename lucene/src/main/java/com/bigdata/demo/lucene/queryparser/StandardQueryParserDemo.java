package com.bigdata.demo.lucene.queryparser;

import com.bigdata.demo.lucene.analyzer.ik.IKAnalyzer4Lucene7;
import com.bigdata.demo.lucene.common.Common;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;

public class StandardQueryParserDemo {
    public static void main(String[] args) throws IOException, QueryNodeException {
        Analyzer analyzer = new IKAnalyzer4Lucene7(true);

        Directory directory = FSDirectory.open(new File(Common.LUCENE_DB_DIR).toPath());
        DirectoryReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);

        StandardQueryParser parser = new StandardQueryParser(analyzer);
        String lql = "(\"联想笔记本电脑\" OR simpleIntro:英特尔) AND type:电脑";
        Query query = parser.parse(lql, "name");
        Common.doQuery(query, searcher);
    }
}
