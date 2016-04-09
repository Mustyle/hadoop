package org.fool.hadoop.hdfs;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Joiner;

public class HdfsTest {

	private FileSystem fs = null;

	@Before
	public void setUp() throws Exception {
		Configuration conf = new Configuration();

		conf.set("fs.defaultFS", "hdfs://hadoop-0000:9000/");

		fs = FileSystem.get(new URI("hdfs://hadoop-0000:9000/"), conf, "hadoop");
	}

	@Test
	public void testMkDir() throws Exception {
		fs.mkdirs(new Path("/aaa/bbb/ccc"));
	}
	
	@Test
	public void testUpload() throws Exception {
		fs.copyFromLocalFile(new Path("pom.xml"), new Path("hdfs://hadoop-0000:9000/pom.xml"));
		fs.copyFromLocalFile(new Path("pom.xml"), new Path("hdfs://hadoop-0000:9000/myupload/pom.xml"));
	}
	
	@Test
	public void testDownload() throws Exception {
		fs.copyToLocalFile(false, new Path("hdfs://hadoop-0000:9000/myupload/pom.xml"), new Path("D:/pom.xml"), true);
	}
	
	@Test
	public void testDelete() throws Exception {
		fs.delete(new Path("/myupload"), true);
		fs.delete(new Path("/pom.xml"), true);
		fs.delete(new Path("/aaa"), true);
	}
	
	@Test
	public void listFiles() throws Exception {
		for(RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true); files.hasNext();) {
			LocatedFileStatus file = files.next();
			
			String name = file.getPath().getName();
			
			System.out.println(name);
		}
		
		System.out.println("\n");
		
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		for (FileStatus fileStatus : listStatus) {
			String name = fileStatus.getPath().getName();
			System.out.println(Joiner.on(" ").join(name, (fileStatus.isDirectory() ? "is dir" : "is file"), fileStatus.getPath()));
		}
	}

}
