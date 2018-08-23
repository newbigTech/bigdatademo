package com.bigdata.demo.lucene.query;

import com.bigdata.demo.lucene.analyzer.ik.IKAnalyzer4Lucene7;
import com.bigdata.demo.lucene.common.Common;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;


public class QueryDemo1 {

    public static void main(String[] args) throws IOException, ParseException {
        // 1. 创建分词器
        Analyzer analyzer = new IKAnalyzer4Lucene7(true);

        // 2. 打开索引存储目录
        Directory directory = FSDirectory.open(new File(Common.LUCENE_DB_DIR).toPath());

        // 3. 索引读取器
        IndexReader reader = DirectoryReader.open(directory);

        // 4. 索引搜索器
        IndexSearcher searcher = new IndexSearcher(reader);

        // 5. 要查询的字段
        String fieldName = "name";

        // 6. 查询生成器
        QueryParser parser = new QueryParser(fieldName, analyzer);

        // 7. 解析输入生成查询对象
        Query query = parser.parse("ThinkPad");

        // 8. 搜索，得到TopN的结果（结果中有命中总数，topN的scoreDocs（评分文档（文档id，评分）））
        // 这里获取的是所有反向索引中的 docId
        TopDocs docs = searcher.search(query, 10);

        System.out.println("总命中数为 : " + docs.totalHits);

        // 遍历反向索引数据，通过 docId 查询数据
        for(ScoreDoc tmp : docs.scoreDocs) {
            Document hitDoc = searcher.doc(tmp.doc);
            System.out.println("命中文档 : " + hitDoc.get(fieldName));
        }

        // 关闭资源
        reader.close();
        directory.close();
    }
}
