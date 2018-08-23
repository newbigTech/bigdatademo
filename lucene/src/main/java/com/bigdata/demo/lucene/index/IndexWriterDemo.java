package com.bigdata.demo.lucene.index;

import com.bigdata.demo.lucene.analyzer.ik.IKAnalyzer4Lucene7;
import com.bigdata.demo.lucene.common.Common;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;

public class IndexWriterDemo {
    public static void main(String[] args) throws IOException {
        Analyzer analyzer = new IKAnalyzer4Lucene7(true);
        // 索引配置
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        // 索引库打开模式, 新建、追加、新建或追加
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

        // 索引库路径
        // FSDirectory - 存放到文件系统中
        Directory dir = FSDirectory.open(
                new File(Common.LUCENE_DB_DIR).toPath());
        // RAMDirectory - 存放到内存中
        //Directory dir = new RAMDirectory();

        // 创建索引写对象
        IndexWriter writer = new IndexWriter(dir, config);

        // 创建文档
        Document doc = new Document();
        // 添加存储字段
        doc.add(new StoredField("prodId", "prod001"));
        String name = "ThinkPad X1 Carbon 20KH0009CD/25CD 超极本轻薄笔记本电脑联想";
        // 文本字段并存储
        doc.add(new TextField("name", name, Field.Store.YES));
        writer.addDocument(doc);

        // 创建文档
        Document doc1 = new Document();
        // 添加存储字段
        doc1.add(new StoredField("prodId", "prod002"));
        String name1 = "ThinkPad X1 Carbon 20KH0009CD/25CD 超极本轻薄笔记本电脑联想";
        // 文本字段并存储
        doc1.add(new TextField("name", name1, Field.Store.YES));

        // 某个 Document 增加了一个属性后, 所有 Doc 都会同意增加但值为空
        String desc = "这是一段非常专业的笔记本描述, 该笔记本太厉害了。";
        doc1.add(new TextField("desc", desc, Field.Store.YES));
        writer.addDocument(doc1);

        // IndexWriter 还提供了 CRUD 的操作方法
        //writer.deleteDocuments(Term term);
        //writer.updateDocument(Term term, IndexableField indexableFiled);

        writer.flush(); // 刷新
        writer.commit(); // 提交
        writer.close(); // 关闭
    }
}
