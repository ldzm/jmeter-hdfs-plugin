package edu.ldzm.jmeter.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.jmeter.reporters.ResultCollector;
import org.apache.jmeter.samplers.SampleSaveConfiguration;
import org.apache.jmeter.save.CSVSaveService;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import edu.ldzm.utils.DateUtils;
import edu.ldzm.utils.Partion;

public class HdfsOperation extends ResultCollector {

	private static final long serialVersionUID = 1L;

	private static final Logger log = LoggingManager.getLoggerForClass();

	public static final String NAME_NODE = "nameNodeTextField";
	public static final String INPUT_FILE_PATH = "inputFilePathTextField";
	public static final String OUTPUT_FILE_PATH = "outputFilePathTextField";
	public static final String INTERVAL = "intervalTextField";

	private boolean running = true;
	public static long beginNum = 0L;
	public static long endNum = 0L;
	public static long lastTimeMillis = 0L;

	private boolean copyFileToHDFS() {

		FileSystem hdfsFileSystem = null;
		Configuration config = new Configuration();
		Exception exception = null;

		try {
			if (StringUtils.isNotBlank(this.getNameNode())
					&& StringUtils.isNotBlank(this.getInputFilePath())
					&& StringUtils.isNotBlank(this.getOutputFilePath())
					&& StringUtils.isNotBlank(getInterval())) {

				config.set("fs.default.name", this.getNameNode().trim());
				hdfsFileSystem = FileSystem.get(config);

				// create tmp directory
				File file = new File("/tmp/jmeter-plugin-"
						+ DateUtils.date2String(new Date()));
				if (file.mkdirs()) {
					log.info("make tmp dirctory" + file.getAbsolutePath()
							+ " successful!");
				} else {
					log.error("make tmp dirctory" + file.getAbsolutePath()
							+ " failure!");
				}

				String filename = "jmeter-" + DateUtils.date2String(new Date())
						+ ".txt";
				File srcFile = new File(this.getInputFilePath().trim());
				File desFile = new File(file.getAbsoluteFile() + "/" + filename);
				Partion partion = new Partion();

				endNum = partion.getLines(srcFile) - 1;
				
				if (endNum >= beginNum) {
					if (partion.partion(srcFile, desFile, beginNum, endNum)) {
						beginNum = endNum + 1;
					} else {
						log.error("file partion failure.");
					}
				} else {
					log.info(srcFile.getAbsolutePath() + " isn't update!");
				}

				Path from = new Path(desFile.getAbsolutePath());
				Path to = new Path(this.getOutputFilePath().trim() + "/"
						+ filename);
				hdfsFileSystem.copyFromLocalFile(from, to);
				log.info("put file:" + from.getName() + " to hdfs successful!");
				desFile.delete();
				log.info("delete " + desFile.getAbsolutePath() + " successful!");
			} else {
				log.info("namenod,input file,output file and interval can not be null.");
			}
		} catch (Exception e) {
			exception = e;
			e.printStackTrace();
		} finally {
			try {
				hdfsFileSystem.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		if (null != exception) {
			return false;
		}

		return true;
	}

	@Override
	public void testStarted() {
		super.testStarted();
		running = true;
		
		Partion partion = new Partion();
		long lines = partion.getLines(new File(getInputFilePath()));
		if (lines > 0) {
			beginNum = lines + 1;
			endNum = lines;
		} else {
			beginNum = 1L;
			endNum = 0L;
		}
		
		File nameListFile = new File(getInputFilePath().trim());
		nameListFile = new File(nameListFile.getParent() + "\namelistfile.txt");
		saveNameList(nameListFile);
		
		lastTimeMillis = System.currentTimeMillis();
		
		new Thread(new Runnable() {

			@Override
			public void run() {
				while (running) {
					long currentTimeMillis = System.currentTimeMillis();
					int seconds = Integer.parseInt(getInterval().trim());
					log.info("interval seconds: " +seconds);
					if (currentTimeMillis - lastTimeMillis > 1000 * 60 * seconds) {
						lastTimeMillis = currentTimeMillis;
						
						if (copyFileToHDFS()) {
							log.info("put file to hdfs successful!");
						} else {
							log.info("put file to fdfs failure!");
						}
					}
				}
			}
		}).start();
	}

	private void saveNameList(File nameListFile) {
		if (!nameListFile.exists()) {
			try {
				nameListFile.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		OutputStream out = null;
		try {
			out = new FileOutputStream(nameListFile);
			try {
				out.write(printableFieldNamesToString().getBytes());
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
			try {
				out.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void testEnded() {
		super.testEnded();

		running = false;

		if (copyFileToHDFS()) {
			log.info("put file to hdfs successful!");
		} else {
			log.info("put file to fdfs failure!");
		}
		
		beginNum = 0L;
		endNum = 0L;
		lastTimeMillis = 0L;
	}

	public String printableFieldNamesToString() {
		return this.printableFieldNamesToString(super.getSaveConfig());
	}

	/**
	 * print file head list
	 * 
	 * @param c
	 *            the configurations you selected
	 * @return
	 */
	public String printableFieldNamesToString(SampleSaveConfiguration c) {
		return CSVSaveService.printableFieldNamesToString(c);
	}

	public HdfsOperation() {
		super();
	}

	public String getNameNode() {
		return this.getPropertyAsString(NAME_NODE);
	}

	public void setNameNode(String value) {
		this.setProperty(NAME_NODE, value);
	}

	public String getInputFilePath() {
		return this.getPropertyAsString(INPUT_FILE_PATH);
	}

	public void setInputFilePath(String value) {
		this.setProperty(INPUT_FILE_PATH, value);
	}

	public String getOutputFilePath() {
		return this.getPropertyAsString(OUTPUT_FILE_PATH);
	}

	public void setOutputFilePath(String value) {
		this.setProperty(OUTPUT_FILE_PATH, value);
	}

	public String getInterval() {
		return this.getPropertyAsString(INTERVAL);
	}

	public void setInterval(String value) {
		this.setProperty(INTERVAL, value);
	}
}
