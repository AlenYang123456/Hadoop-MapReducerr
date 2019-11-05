package com.hadoop.wordcount;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: dreamer Q
 * @Date: 2019/11/4 22:26
 * @Version 1.0
 * @Discription 使用MapReduce 开发 WordCount应用程序
 */
public class WordCount2App{


    /**
     * Map: 读取输入的文件
     * Mapper里面四个泛型的含义
     * 输入的数据类型:
     *      LongWritable:表示偏移量,由于Long要在服务器之间传输,LongWritable是hadoop封装的可序列的类型
     *      Text:文本类型
     * 输出的数据类型:
     *      Text:文本类型
     *      LongWritable:表示统计的数量,同Long一样,要在服务器之间传输,IntWritable是hadoop封装的可序列化的类型
     */
    public static class WCMapper extends Mapper<LongWritable,Text,Text,LongWritable>{


        //mapreduce框架每读一行数据就调用一次该方法
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //具体的业务逻辑就卸载这个方法中,而且我们业务要处理的数据已经被框架传递进来,在方法的参数中key-value
            //key 是这一行数据的起始偏移量,value 是这一行的文本内容

            //将这一行的内容换成String类型
            String line = value.toString();

            //对这一行的文本按特定分隔符切分
            String[] words = StringUtils.split(line, " ");

            //遍历这个单词数据输出为key-value的形式 k:单词 v :1
            for (String word : words) {
                context.write(new Text(word), new LongWritable(1));
            }

        }
    }

    /**
     * Reduce:归并操作
     */
    public static class WCReducer extends Reducer<Text,LongWritable,Text,LongWritable>{

        //框架在map处理完成之后,将所有kv对缓存起来,进行分组,然后传递一个组<key,values{}> 调用一次reduce方法
        //<hello,{1,1,1,1,1,....}>
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count=0;

            for (LongWritable value : values) {
                count +=value.get();
            }

            //输出这一个单词的统计结果
            context.write(key, new LongWritable(count));
        }
    }

    /**
     * 用来描述一个特定的作业
     * 比如:改作业使用那个类作为逻辑处理中的map,哪个作为reduce
     * 还可以指定该作业要处理的数据所在的路径
     * 还可以指定该作业输出的结果的结果放到哪个路径
     * @param args
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

    //   Job job = new Job(); 已过时
        Configuration conf =new Configuration();

        Job job = Job.getInstance(conf);

        //设置整个job所有的那些类所用的jar包的位置-因为是分布式
        job.setJarByClass(WordCount2App.class);


        //本job使用的mapper和reducer的类
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);

        //指定reduce 的输出 key 和 value 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //指定mapper的输出数据 key 和 value 类型

        //指定要输入的数据存放的路径--输入的数据路径
        FileInputFormat.setInputPaths(job, new Path("/wc/srcdata"));

        //指定处理结果的输出数据存放路径--输出的数据路径
        FileOutputFormat.setOutputPath(job, new Path("/wc/output"));

        //将job提交给集群运行
        job.waitForCompletion(true);

    }

}
