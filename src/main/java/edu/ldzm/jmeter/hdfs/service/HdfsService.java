package edu.ldzm.jmeter.hdfs.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

public class HdfsService {

	private static final Logger log = LoggingManager.getLoggerForClass();

	public static boolean startMapReduce(String hadoopCmd, String jobPath, String inputDirPath, String outputDirPath,
			List<String> args) {
		String shell = hadoopCmd + " jar " + jobPath + " " + inputDirPath + " " + outputDirPath + " " + toString(args);
		String[] cmds = { "/bin/bash", "-c", shell };
		log.info(shell);
		try {
			Process process = Runtime.getRuntime().exec(cmds);
			int flag = process.waitFor();
			if (0 == flag) {
				return true;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return false;
	}

	private static String toString(List<String> args) {

		String result = "";
		for (int i = 0; i < args.size(); i++) {
			result += args.get(i) + " ";
		}
		return result;
	}

	public static void main(String[] args) {
		List<String> list = new ArrayList<String>();
		list.add("-l");
		list.add("timeStamp,elapsed,label,responseCode,responseMessage,threadName,dataType,success,bytes,grpThreads,allThreads,Latency");
		boolean flag = HdfsService
				.startMapReduce(
						"/home/sky/local/program/hadoop-0.19.0/bin/hadoop",
						"/home/sky/Desktop/pt/analysis_summery.jar",
						"hdfs://sky:9000/art/input",
						"hdfs://sky:9000/art/output01",	list);
		System.out.println(flag);
	}
}
