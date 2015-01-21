package edu.ldzm.jmeter.hdfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
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

import edu.ldzm.utils.DateUtil;
import edu.ldzm.utils.FileUtil;
import edu.ldzm.utils.Partion;

public class HdfsOperation extends ResultCollector {

	private static final long serialVersionUID = 1L;

	private static final Logger log = LoggingManager.getLoggerForClass();

	public static final String NAME_NODE = "nameNodeTextField";
	public static final String INPUT_FILE_PATH = "inputFilePathTextField";
	public static final String OUTPUT_FILE_PATH = "outputFilePathTextField";
	public static final String INTERVAL = "intervalTextField";
	
	public static final int CONNECT_TIMES = 10;

	private boolean running = true;
	public static long beginNum = 0L;
	public static long endNum = 0L;
	public static long lastTimeMillis = System.currentTimeMillis();

	private boolean copyFileToHDFS() {

		FileSystem hdfsFileSystem = null;
		Configuration config = new Configuration();
		Exception exception = null;
		config.set("fs.default.name", this.getNameNode().trim());
		
		for (int i = 0; i < CONNECT_TIMES; i++) {
			try {
				hdfsFileSystem = FileSystem.get(config);
			} catch (IOException e1) {
				exception = e1;
				e1.printStackTrace();
			}
			if (null != hdfsFileSystem) {
				break;
			}
		}
		
		if (null == hdfsFileSystem) {
			log.error("Connect to FileSystem failure.");
		}

		try {
			if (StringUtils.isNotBlank(this.getNameNode())
					&& StringUtils.isNotBlank(this.getInputFilePath())
					&& StringUtils.isNotBlank(this.getOutputFilePath())
					&& StringUtils.isNotBlank(getInterval())) {

				// create tmp directory
				File file = new File("/tmp/jmeter-plugin-"
						+ DateUtil.date2String(new Date()));
				FileUtil.createDir(file);

				String filename = "jmeter-" + DateUtil.date2String(new Date())
						+ ".txt";
				File srcFile = new File(this.getInputFilePath().trim());
				File desFile = new File(file.getAbsoluteFile() + "/" + filename);
				FileUtil.createFile(desFile);
				
				Partion partion = new Partion();

				endNum = partion.getLines(srcFile) - 1;
				
				if (endNum >= beginNum) {
					if (partion.partion(srcFile, desFile, beginNum, endNum)) {
						beginNum = endNum + 1;
					} else {
						log.error("file partion failure.");
					}
					
					Path from = new Path(desFile.getAbsolutePath());
					Path to = new Path(this.getOutputFilePath().trim() + "/"
							+ filename);
					hdfsFileSystem.copyFromLocalFile(from, to);
					log.info("put file:" + from.getName() + " to hdfs successful!");
					desFile.delete();
					log.info("delete " + desFile.getAbsolutePath() + " successful!");
				} else {
					log.info(srcFile.getAbsolutePath() + " isn't update!");
					return false;
				}
			} else {
				log.info("namenod,input file,output file and interval can not be null.");
				return false;
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
		nameListFile = new File(nameListFile.getParent() + "/namelistfile.txt");
		saveNameList(nameListFile);
		
		final int seconds = Integer.parseInt(getInterval().trim());
		log.info("interval seconds: " +seconds);
		lastTimeMillis = System.currentTimeMillis();
		
		new Thread(new Runnable() {

			@Override
			public void run() {
				while (running) {
					long currentTimeMillis = System.currentTimeMillis();

					if (currentTimeMillis - lastTimeMillis > 1000 * seconds) {
						lastTimeMillis = currentTimeMillis;
						
						if (copyFileToHDFS()) {
							log.info("put file to hdfs successful!");
						}
					}
				}
			}
		}).start();
	}

	private void saveNameList(File nameListFile) {
		
		FileUtil.createFile(nameListFile);
		OutputStream out = null;
		try {
			out = new FileOutputStream(nameListFile);
			try {
				out.write(printableFieldNamesToString().getBytes());
			} catch (IOException e) {
				e.printStackTrace();
			}
			log.info("save file head to " + nameListFile.getAbsolutePath() + " successful.");
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
		}
		
		beginNum = 0L;
		endNum = 0L;
		lastTimeMillis = System.currentTimeMillis();
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
