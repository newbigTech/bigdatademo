package com.bigdata.demo.lucene.analyzer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeReflector;

import java.io.IOException;

public class MyWhitespaceAnalyzer extends Analyzer {

    /**
     * 需要继承 Analyzer 并重写 createComponents 方法
     * @param fieldName
     * @return
     */
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer source = new MyWhitespaceTokenizer();
        TokenStream filter = new MyLowerCaseTokenFilter(source);
        return new TokenStreamComponents(source, filter);
    }

    /**
     * 源 - 用来分词
     */
    public static class MyWhitespaceTokenizer extends Tokenizer {

        // 通过方法获取 Attribute
        private MyCharAttribute attr = this.addAttribute(MyCharAttribute.class);

        // 用来存储被分出的词项
        private char[] buffer = new char[255];
        // 词的长度
        private int length = 0;
        // 字节流中读出的每个字符
        private int ch;

        /**
         * 重写 incrementToken 方法
         * @return
         * @throws IOException
         */
        @Override
        public boolean incrementToken() throws IOException {
            // 因为是重用, 所以每次使用前必须清空上一次的数据
            this.clearAttributes();
            length = 0;
            while(true) {
                if(-1 == (ch = this.input.read())) {
                    if(length > 0) {
                        // 把词项复制到 Attribute 中
                        attr.setChars(buffer, length);
                        return true;
                    } else {
                        return false;
                    }
                }

                // 空白字符时需要分词
                if(Character.isWhitespace(ch)) {
                    if(length > 0) {
                        // 把词项复制到 Attribute 中
                        attr.setChars(buffer, length);
                        return true;
                    }
                }

                buffer[length++] = (char) ch;
            }
        }
    }

    /**
     * 词项过滤器, 用来对分词后的词项做处理
     */
    public static class MyLowerCaseTokenFilter extends TokenFilter {

        /**
         * input 用来传入需要过滤的 Tokenizer 或另一个 TokenFilter
         * @param input
         */
        public MyLowerCaseTokenFilter(TokenStream input) {
            super(input);
        }

        private MyCharAttribute attr = this.getAttribute(MyCharAttribute.class);

        @Override
        public boolean incrementToken() throws IOException {
            // 链式调用, 调用上一个 TokenStream 的 incrementToken 方法
            boolean resp = this.input.incrementToken();
            // 返回 true 表示处理成功
            if(resp) {
                char[] buffer = attr.getChars();
                int length = attr.getLength();
                if(length > 0) {
                    for(int i = 0; i < length; i++) {
                        buffer[i] = Character.toLowerCase(buffer[i]);
                    }
                }
            }

            return resp;
        }
    }

    /**
     * 词项属性接口需要继承 Attribute
     */
    interface MyCharAttribute extends Attribute {
        public void setChars(char[] buffer, int length);

        public char[] getChars();

        public int getLength();

        public String getString();
    }

    /**
     * 1. 实现类需要继承 AttributeImpl
     * 2. 需要实现自定义接口类
     * 3. 命名规则 接口名+Impl
     * 4. 必须要有无参构造器
     */
    public static class MyCharAttributeImpl extends AttributeImpl
            implements MyCharAttribute {

        private char[] charTerm = new char[255];
        private int length = 0;

        @Override
        public void setChars(char[] buffer, int length) {
            this.length = length;
            if(this.length > 0) {
                System.arraycopy(buffer, 0, charTerm, 0, length);
            }
        }

        @Override
        public char[] getChars() {
            return this.charTerm;
        }

        @Override
        public int getLength() {
            return this.length;
        }

        @Override
        public String getString() {
            if(this.length > 0){
                return new String(charTerm, 0, length);
            }
            return null;
        }

        @Override
        public void clear() {
            this.length = 0;
        }

        @Override
        public void reflectWith(AttributeReflector reflector) {

        }

        @Override
        public void copyTo(AttributeImpl target) {

        }
    }

    public static void main(String[] args) {
        String text = "An AttributeSource contains a list of different AttributeImpls, and methods to add and get them. ";

        try (
                Analyzer analyzer = new MyWhitespaceAnalyzer();
                TokenStream ts = analyzer.tokenStream("", text);
                ) {
            // 获取词项属性
            MyCharAttribute attr = ts.getAttribute(MyCharAttribute.class);
            // 重置
            ts.reset();
            while(ts.incrementToken()) {
                System.out.printf(attr.getString() + "|");
            }
            ts.end();
            System.out.println();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
