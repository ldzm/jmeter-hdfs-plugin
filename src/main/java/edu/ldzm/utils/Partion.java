package edu.ldzm.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

public class Partion {
	private static final Logger log = LoggingManager.getLoggerForClass();
	
	/**
	 * copy the content of srcFile to desFile
	 * 
	 * @param srcFile
	 *            source file
	 * @param desFile
	 *            destination file
	 * @param beginNum
	 *            the number of begin to copy
	 * @param endNum
	 *            the end
	 * @return
	 */
	public boolean partion(File srcFile, File desFile, long beginNum,
			long endNum) {
		String shell = "sed -n '" + beginNum + "," + endNum + "p' "
				+ srcFile.getAbsolutePath() + ">" + desFile.getAbsolutePath();
		log.info(shell);
		String[] cmds = { "/bin/bash", "-c", shell };

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

	/**
	 * get the lines of the file
	 * 
	 * @param srcFile
	 *            the source file
	 * @return the file's lines
	 */
	public long getLines(File srcFile) {

		String shell = "wc -l " + srcFile.getAbsolutePath()
				+ " | awk '{print $1}'";
		String[] cmds = { "/bin/bash", "-c", shell };

		log.info(shell);
		long result = 0L;
		BufferedReader br = null;
		try {
			Process process = Runtime.getRuntime().exec(cmds);
			process.waitFor();
			InputStream is = process.getInputStream();
			InputStreamReader isr = new InputStreamReader(is);
			br = new BufferedReader(isr);
			String value = br.readLine();
			result = Long.valueOf(new String(value));
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		log.info("lines of file " + srcFile.getAbsolutePath() + " is " + result);
		
		return result;
	}
}
