package com.bigdata.demo.lucene.exercise;

import com.bigdata.demo.lucene.analyzer.ik.IKAnalyzer4Lucene7;
import com.bigdata.demo.lucene.common.Common;
import com.bigdata.demo.lucene.field.IntField;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.File;
import java.io.IOException;

public class ProductIndexExercise {
    public static void main(String[] args) {
        String prodId = "p0002";

        String name = "ThinkPad X1 Carbon 20KH0009CD/25CD 超极本轻薄联想笔记本";

        String imgUrl = "http://www.dongnao.com/aaa";

        String simpleIntro = "集成显卡 英特尔 酷睿 i5-8250U 14英寸";

        String brand = "ThinkPad";

        String[] types = new String[]{"电脑", "笔记本电脑"};

        int price = 1111100; // 价格 - 单位: 分

        Analyzer analyzer = new IKAnalyzer4Lucene7(true);
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

        try(
                Directory dir = FSDirectory.open(new File(Common.LUCENE_DB_DIR).toPath());
                IndexWriter writer = new IndexWriter(dir, config);
                ) {
            Document doc = new Document();
            // 商品id：字符串，不索引、但存储
            FieldType onlyStoredType = new FieldType();
            onlyStoredType.setTokenized(false);
            onlyStoredType.setIndexOptions(IndexOptions.NONE);
            onlyStoredType.setStored(true);
            onlyStoredType.freeze();
            doc.add(new Field("prodId", prodId, onlyStoredType));

            // 商品名称：字符串，分词索引(存储词频、位置、偏移量)、存储
            FieldType indexedAllStoredType = new FieldType();
            indexedAllStoredType.setStored(true);
            indexedAllStoredType.setTokenized(true);
            indexedAllStoredType.setIndexOptions(IndexOptions
                    .DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
            indexedAllStoredType.freeze();
            doc.add(new Field("name", name, indexedAllStoredType));

            // 图片链接：仅存储
            doc.add(new Field("imgUrl", imgUrl, onlyStoredType));

            // 商品简介：文本，分词索引（不需要支持短语、临近查询）、存储，结果中支持高亮显示
            FieldType indexedTermVectorsStoredType = new FieldType();
            indexedTermVectorsStoredType.setStored(true);
            indexedTermVectorsStoredType.setTokenized(true);
            indexedTermVectorsStoredType.setIndexOptions(IndexOptions
                    .DOCS_AND_FREQS);
            indexedTermVectorsStoredType.setStoreTermVectors(true);
            indexedTermVectorsStoredType.setStoreTermVectorPositions(true);
            indexedTermVectorsStoredType.setStoreTermVectorOffsets(true);
            indexedTermVectorsStoredType.freeze();
            doc.add(new Field("simpleIntro", simpleIntro
                    , indexedTermVectorsStoredType));

            // 品牌：字符串，不分词索引，存储，可按品牌排序、查询
            FieldType indexedNotTokenizeNotStoredType = new FieldType();
            indexedNotTokenizeNotStoredType.setStored(true);
            indexedNotTokenizeNotStoredType.setTokenized(false);
            indexedNotTokenizeNotStoredType.setIndexOptions(IndexOptions.DOCS);
            // 有排序、分类查询的需要，建立docValues正向索引
            indexedNotTokenizeNotStoredType
                    .setDocValuesType(DocValuesType.SORTED);
            indexedNotTokenizeNotStoredType.freeze();
            // 建立 docValues 需要重写 binaryValue() 方法
            doc.add(new Field("brand", brand
                    , indexedNotTokenizeNotStoredType) {
                @Override
                public BytesRef binaryValue() {
                    return new BytesRef((String) this.fieldsData);
                }
            });

            // 类别：字符串，索引不分词，不存储、支持分类统计,多值
            FieldType indexedDocValuesType = new FieldType();
            indexedDocValuesType.setStored(false);
            indexedDocValuesType.setTokenized(false);
            indexedDocValuesType.setIndexOptions(IndexOptions.DOCS);
            indexedDocValuesType.setDocValuesType(DocValuesType.SORTED_SET);
            indexedDocValuesType.freeze();

            doc.add(new Field("type", types[0], indexedDocValuesType) {
                @Override
                public BytesRef binaryValue() {
                    return new BytesRef((String) this.fieldsData);
                }
            });
            doc.add(new Field("type", types[1], indexedDocValuesType) {
                @Override
                public BytesRef binaryValue() {
                    return new BytesRef((String) this.fieldsData);
                }
            });

            // 价格，整数，单位分，不索引、存储、要支持排序
            FieldType numericDocValuesType = new FieldType();
            numericDocValuesType.setTokenized(false);
            numericDocValuesType.setIndexOptions(IndexOptions.NONE);
            numericDocValuesType.setStored(true);
            numericDocValuesType.setDocValuesType(DocValuesType.NUMERIC);
            numericDocValuesType.setDimensions(1, Integer.BYTES);
            numericDocValuesType.freeze();
            doc.add(new IntField("price", price, numericDocValuesType));

            writer.addDocument(doc);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}