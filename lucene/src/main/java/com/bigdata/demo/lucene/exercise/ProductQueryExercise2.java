/*
 * <p>
 * Copyright: Copyright (c) 2018-08-15 16:21
 * <p>
 * Company: 武汉斑马快跑
 * <p>
 *
 * @author tongyongjian
 * @email tongyongjian@bmkp.cn
 * @version 1.0.0
 */

package com.bigdata.demo.lucene.exercise;

import com.bigdata.demo.lucene.common.Common;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;

public class ProductQueryExercise2 {

    public static void main(String[] args) throws IOException {
        Directory directory = FSDirectory.open(new File(Common.LUCENE_DB_DIR).toPath());
        DirectoryReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);

        Query nameQuery = new PhraseQuery("name", "联想", "笔记本电脑");
        Query introQuery = new PhraseQuery("simpleIntro", "联想笔记本电脑");
        Query typeQuery = new TermQuery(new Term("type", "电脑"));
        Query priceQuery = IntPoint.newRangeQuery("price", 800000
                , 10000000);

        // q1 or q2 and q3 and q4
        System.out.println("****************** 布尔查询 ******************");
        BooleanQuery booleanQuery = new BooleanQuery.Builder()
                .add(nameQuery, BooleanClause.Occur.SHOULD)
                .add(introQuery, BooleanClause.Occur.SHOULD)
                .add(typeQuery, BooleanClause.Occur.MUST)
                .add(priceQuery, BooleanClause.Occur.MUST).build();

        Common.doQuery(booleanQuery, searcher);

        // (q1 or q2) and q3 and q4
        System.out.println("****************** 嵌套布尔查询 ******************");
        BooleanQuery booleanQuery1 = new BooleanQuery.Builder()
                .add(nameQuery, BooleanClause.Occur.SHOULD)
                .add(introQuery, BooleanClause.Occur.SHOULD).build();
        BooleanQuery booleanQuery2 = new BooleanQuery.Builder()
                .add(booleanQuery1, BooleanClause.Occur.MUST)
                .add(typeQuery, BooleanClause.Occur.MUST)
                .add(priceQuery, BooleanClause.Occur.MUST).build();
        Common.doQuery(booleanQuery2, searcher);
    }
}
