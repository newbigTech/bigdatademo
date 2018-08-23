package com.bigdata.demo.lucene.exercise;

import com.bigdata.demo.lucene.common.Common;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;


public class ProductQueryExercise {

    public static void main(String[] args) throws IOException {
        Directory directory = FSDirectory.open(new File(Common.LUCENE_DB_DIR).toPath());
        DirectoryReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);

        Query nameQuery = new PhraseQuery("name", "联想", "笔记本电脑");
        Query introQuery = new PhraseQuery("simpleIntro", "联想笔记本电脑");
        BooleanQuery booleanQuery = new BooleanQuery.Builder()
                .add(nameQuery, BooleanClause.Occur.SHOULD)
                .add(introQuery, BooleanClause.Occur.SHOULD).build();

        Common.doQuery(booleanQuery, searcher);
    }

}
