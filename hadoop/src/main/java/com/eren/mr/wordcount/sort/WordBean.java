package com.eren.mr.wordcount.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordBean implements WritableComparable<WordBean> {

    private String word;

    private Integer num;

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Integer getNum() {
        return num;
    }

    public void setNum(Integer num) {
        this.num = num;
    }

    @Override
    public String toString() {
        return "" + word + " " + num;
    }

    /**
     * 比较器，按照自己定义的规则进行排序
     * 排序规则
     * 按照第一列字母顺序排序
     * 如果第一列字母相同按照第二列的数字大小排序
     *
     * @param o
     * @return
     */
    @Override
    public int compareTo(WordBean o) {
        //1.比较第一列
        int result = this.word.compareTo(o.getWord());
        //2.比较第二列
        if (result == 0) {
            result = this.num.compareTo(o.getNum());
        }
        return result;
    }

    /**
     * 实现 序列化
     *
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(word);
        out.writeInt(num);
    }

    /**
     * 实现反序列化
     *
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.word = in.readUTF();
        this.num = in.readInt();
    }
}
