package com.sist.spark;


import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.InputStreamReader;

import org.apache.commons.collections.functors.WhileClosure;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MainClass {

	public static void main(String[] args) {
		try {
			Configuration conf=new Configuration();
			conf.set("fs.default.name", "hdfs://NameNode:9000");//java에서는 localhost가 아니라 NameNode이다.
			
			FileSystem fs=FileSystem.get(conf);
			FileStatus[] status=fs.listStatus(new Path("/user/flume/tweeter_data_ns1"));//-ls에 해당
			
			StringBuffer data=new StringBuffer();
			for (FileStatus s : status) {
				
				if (s.getPath().getName().endsWith("tmp")) {//저장하다 실패한파일을 tmp, 이내용을 카프카에서 받아올 수 있다.
					continue;
				}
				System.out.println(s.getPath().getName());
				
				FSDataInputStream is=fs.open(new Path("/user/flume/tweeter_data_ns1/"+s.getPath().getName()));//FileReader하둡파일리더
				BufferedReader br=new BufferedReader(new InputStreamReader(is));
				
				while (true) {
					String line=br.readLine();
					if (line==null) {
						break;
					}
					data.append(line+"\n");
					//System.out.println(data.toString());
				}
				br.close();
			}
			System.out.println(data.toString());
			String result=data.toString().replaceAll("[^가-힣]", "");
			FileWriter fw=new FileWriter("./twitter.txt");
			fw.write(result);
			fw.close();
			
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
			
		}


	}

}
