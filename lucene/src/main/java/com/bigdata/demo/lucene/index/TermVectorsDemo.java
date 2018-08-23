package com.bigdata.demo.lucene.index;

import com.bigdata.demo.lucene.analyzer.ik.IKAnalyzer4Lucene7;
import com.bigdata.demo.lucene.common.Common;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;

public class TermVectorsDemo {
    public static void main(String[] args) {
        Analyzer analyzer = new IKAnalyzer4Lucene7(true);
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

        try(
                Directory dir = FSDirectory.open(new File(Common.LUCENE_DB_DIR).toPath());

                IndexWriter writer = new IndexWriter(dir, config);
                ) {
            Document doc = new Document();
            String name = "content";
            String value = "联想高性能电脑，Mac系统4G内存1T硬盘";
            FieldType type = new FieldType();
            type.setStored(true);
            type.setTokenized(true);
            type.setIndexOptions(IndexOptions.DOCS);

            type.setStoreTermVectors(true);
            type.setStoreTermVectorOffsets(true);
            type.setStoreTermVectorPayloads(true);
            type.setStoreTermVectorPositions(true);

            type.freeze();

            Field field = new Field(name, value, type);
            doc.add(field);
            writer.addDocument(doc);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
