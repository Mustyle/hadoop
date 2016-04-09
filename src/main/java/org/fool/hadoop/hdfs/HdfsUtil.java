package org.fool.hadoop.hdfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

public class HdfsUtil {

	private FileSystem fs = null;

	@Before
	public void setUp() throws Exception {
		// 读取classpath下的xxx-site.xml 配置文件，并解析其内容，封装到conf对象中
		Configuration conf = new Configuration();

		// 也可以在代码中对conf中的配置信息进行手动设置，会覆盖掉配置文件中的读取的值
		conf.set("fs.defaultFS", "hdfs://hadoop-0000:9000/");

		// 根据配置信息，去获取一个具体文件系统的客户端操作实例对象
		fs = FileSystem.get(new URI("hdfs://hadoop-0000:9000/"), conf, "hadoop");
	}

	/**
	 * 上传文件，比较底层的写法 添加VM参数 -DHADOOP_USER_NAME=hadoop
	 */
	@Test
	public void upload() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoop-0000:9000/");

		FileSystem fs = FileSystem.get(conf);

		FSDataOutputStream os = fs.create(new Path("hdfs://hadoop-0000:9000/aa/pom.xml"));

		BufferedInputStream is = new BufferedInputStream(new FileInputStream(new File("pom.xml")));

		IOUtils.copy(is, os);
	}

	/**
	 * 上传文件，封装好的写法
	 */
	@Test
	public void upload2() throws Exception {
		fs.copyFromLocalFile(new Path("pom.xml"), new Path("hdfs://hadoop-0000:9000/aa/pom.xml"));
	}
	
	/**
	 * 下载文件
	 */
	@Test
	public void download() throws Exception {
		fs.copyToLocalFile(false, new Path("hdfs://hadoop-0000:9000/aa/pom.xml"), new Path("C:/pom.xml"), true);
	}

	/**
	 * 查看文件信息
	 */
	@Test
	public void listFiles() throws Exception {
		// listFiles列出的是文件信息，而且提供递归遍历
		RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);

		while (files.hasNext()) {
			LocatedFileStatus file = files.next();
			Path path = file.getPath();
			String name = path.getName();

			System.out.println(name);
		}

		System.out.println("---------------------------------");

		// listStatus 可以列出文件和文件夹的信息，但是不提供自带的递归遍历
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		for (FileStatus status : listStatus) {
			String name = status.getPath().getName();
			System.out.println(name + (status.isDirectory() ? " is dir" : " is file"));
		}
	}

	/**
	 * 创建文件夹
	 */
	@Test
	public void mkdir() throws Exception {
		fs.mkdirs(new Path("/aaa/bbb/ccc"));
	}
	
	/**
	 * 删除文件或文件夹
	 */
	@Test
	public void rm() throws Exception {
		fs.delete(new Path("/aa"), true);
	}

	public static void main(String[] args) throws Exception {
		// download a file from hdfs
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoop-0000:9000/");

		FileSystem fs = FileSystem.get(conf);

		FSDataInputStream is = fs.open(new Path("/hadoop-2.6.4.tar.gz"));

		BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(new File("C:/hadoop.tgz")));

		IOUtils.copy(is, os);
	}
}
