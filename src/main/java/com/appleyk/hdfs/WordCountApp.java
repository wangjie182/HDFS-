package com.appleyk.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.appleyk.hdfs.mapper.WordCountMapper;
import com.appleyk.hdfs.reducer.WordCountReducer;

/**
 * MapReduce任务的Client端，主要用来提交Job
 * @author yukun24@126.com
 * @blob   http://blog.csdn.net/appleyk
 * @date   2018年7月3日-上午9:51:49
 */
public class WordCountApp {
	private static HdfsApi api;
	private static FileSystem fs = null;

	/**
	 * 初始化Api
	 * 
	 * @throws Exception
	 */
	public static void initApi() throws Exception {

		Configuration conf = new Configuration();

		/**
		 * dfs.client.block.write.replace-datanode-on-failure.enable=true
		 * 如果在写入管道中存在一个DataNode或者网络故障时， 那么DFSClient将尝试从管道中删除失败的DataNode，
		 * 然后继续尝试剩下的DataNodes进行写入。 结果，管道中的DataNodes的数量在减少。 enable ：启用特性，disable：禁用特性
		 * 该特性是在管道中添加新的DataNodes。
		 * 当集群规模非常小时，例如3个节点或更少时，集群管理员可能希望将策略在默认配置文件里面设置为NEVER或者禁用该特性。
		 * 否则，因为找不到新的DataNode来替换，用户可能会经历异常高的管道写入错误,导致追加文件操作失败
		 */
		conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");

		/**
		 * dfs.client.block.write.replace-datanode-on-failure.policy=DEFAULT
		 * 这个属性只有在dfs.client.block.write.replace-datanode-on-failure.enable设置true时有效：
		 * ALWAYS ：当一个存在的DataNode被删除时，总是添加一个新的DataNode NEVER ：永远不添加新的DataNode
		 * DEFAULT：副本数是r，DataNode的数时n，只要r >= 3时，或者floor(r/2)大于等于n时
		 * r>n时再添加一个新的DataNode，并且这个块是hflushed/appended
		 */
		conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");

		/**
		 * 本地开启HDFS的回收站功能，设置回收站的文件过期时间为360/60 = 6个小时 如果HDFS开启的有回收站功能，以其设置的interval为准
		 */
		conf.set("fs.trash.interval", "360");

		/**
		 * 设置hdfs文件系统的uri(远程连接)
		 */
		conf.set("fs.defaultFS", "hdfs://1.1.1.5:9000");
		api = new HdfsApi(conf, "root");

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		// 配置uri
		conf.set("fs.defaultFS", "hdfs://1.1.1.5:9000");

		// 创建一个作业，作用在Hadoop集群上（remote）
		Job job = Job.getInstance(conf, "wordCount");

		/**
		 * 设置jar包的主类（如果样例demo打成Jar包扔在Linux下跑任务， 需要指定jar包的Main
		 * Class,也就是指定jar包运行的主入口main函数）
		 */
		job.setJarByClass(WordCountApp.class);

		// 设置Mapper 任务的类（自己写demo实现map）
		job.setMapperClass(WordCountMapper.class);
		// 设置Reducer任务的类（自己写demo实现reduce）
		job.setReducerClass(WordCountReducer.class);

		// 指定mapper的分区类
		// job.setPartitionerClass(PartitionTest.class);

		// 设置reducer（reduce task）的数量（从0开始）
		// job.setNumReduceTasks(2);

		// 设置映射输出数据的键（key） 类（型）
		job.setMapOutputKeyClass(Text.class);
		// 设置映射输出数据的值（value）类（型）
		job.setMapOutputValueClass(IntWritable.class);

		// 设置作业（Job）输出数据的键（key） 类（型） == 最后要写入到输出文件里面
		job.setOutputKeyClass(Text.class);
		// 设置作业（Job）输出数据的值（value）类（型） == 最后要写入到输出文件里面
		job.setOutputValueClass(IntWritable.class);

		initApi();
		api.upLoadFile("D:\\eclipse-workspace\\spider_website_info-master3\\data\\webmagic\\20200127.txt",
		"/input/", true, true);

		// 设置输入的Path列表（可以是单个文件也可以是多个文件（目录表示即可））
		 FileInputFormat.setInputPaths(job, new Path("hdfs://1.1.1.5:9000/input/20200127.txt"));
		
		//判断文件是否存在
		boolean result = api.existFile("output");
		if(result){
			//参数依次为：要删除的文件或目录/是否递归删除（针对目录）/是否跳过回收站（如果true，表示直接、彻底删除）
			boolean result2 = api.rmdir("output", true, true);
			System.out.println("删除状态：" + result2);
		}
		api.close();

		//设置输出的目录Path（确认输出Path不存在，如存在，请先进行目录删除）
		FileOutputFormat.setOutputPath(job, new Path("hdfs://1.1.1.5:9000/output"));

		//将作业提交到集群并等待它完成。
		boolean bb =job.waitForCompletion(true);
		
		if (!bb) {
			System.out.println("Job任务失败！");
		} else {
			System.out.println("Job任务成功！");
		}
	}

}

// File file = new File("");
// if(file.exists()){
// 	File[] files = file.listFiles();
// for(File file2 :files){
	// if(!file2.isDirectory()){
		//api.upLoad(); 
	// }
// }
// }