/*
 * <p>
 * Copyright: Copyright (c) 2018-08-16 17:21
 * <p>
 * Company: 武汉斑马快跑
 * <p>
 *
 * @author tongyongjian
 * @email tongyongjian@bmkp.cn
 * @version 1.0.0
 */

package com.bigdata.demo.lucene.queryparser;

import com.bigdata.demo.lucene.analyzer.ik.IKAnalyzer4Lucene7;
import com.bigdata.demo.lucene.common.Common;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;

public class MultiFieldQueryParserDemo {

    public static void main(String[] args) throws IOException, ParseException {
        Analyzer analyzer = new IKAnalyzer4Lucene7(true);

        Directory directory  = FSDirectory.open(new File(Common.LUCENE_DB_DIR).toPath());
        DirectoryReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);

        String[] multiDefault = {"name", "simpleIntro", "type"};
        MultiFieldQueryParser parser = new MultiFieldQueryParser(multiDefault, analyzer);
        // 多个默认字段使用 OR 连接
        String lql = "联想笔记本 AND type:电脑";
        Query query = parser.parse(lql);
        Common.doQuery(query, searcher);
    }

}
