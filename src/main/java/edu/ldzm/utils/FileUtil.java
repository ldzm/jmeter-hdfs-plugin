package edu.ldzm.utils;

import java.io.File;
import java.io.IOException;

import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

public class FileUtil {

	private static final Logger log = LoggingManager.getLoggerForClass();
	
	public static boolean createFile(File src) {
		
		boolean result = false;
		if (!src.exists()) {
			try {
				result = src.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
			log.info("create file " + src.getAbsolutePath() + " successful.");
		} else {
			result = true;
		}
		
		if (!result) {
			log.info("create file " + src.getAbsolutePath() + " failure.");
		}

		return result;
	}
	
	public static boolean createDir(File src) {
		
		boolean result = false;
		if (!src.exists()) {
			result = src.mkdirs();
			log.info("create directory " + src.getAbsolutePath() + " successful.");
		} else {
			result = true;
		}
		
		if (!result) {
			log.info("create directory " + src.getAbsolutePath() + " failure.");
		}
		
		return result;
	}
}
