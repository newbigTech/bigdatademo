package com.bigdata.demo.webmagic.csdn;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class CsdnBlog {
        private int id;// 编号
        private String title;// 标题
        private String date;// 日期
        private String tags;// 标签
        private String category;// 分类
        private int view;// 阅读人数
        private int comments;// 评论人数
        private int copyright;// 是否原创

    }
