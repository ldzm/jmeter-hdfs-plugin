package edu.ldzm.jmeter.hdfs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.jmeter.reporters.ResultCollector;
import org.apache.jmeter.samplers.SampleEvent;
import org.apache.jmeter.samplers.SampleSaveConfiguration;
import org.apache.jmeter.save.CSVSaveService;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

public class HdfsOperation extends ResultCollector {
    
    private static final long serialVersionUID = 1L;
    public static final String HDFS_PATH = "hdfs://sky:9000";
    public static final String DIR_PATH = "/d1000";
    public static final String FILE_PATH = "/d1000/f1000";
    
    private static final Logger log = LoggingManager.getLoggerForClass();
    
    public static final String NAME_NODE = "nameNodeTextField";
    public static final String INPUT_FILE_PATH = "inputFilePathTextField";
    public static final String OUTPUT_FILE_PATH = "outputFilePathTextField";
    public static final String INTERVAL = "intervalTextField";

    @Override
    public void sampleOccurred(SampleEvent e) {
        
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(new URI(HDFS_PATH), new Configuration());
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        } catch (URISyntaxException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        
        uploadFile(fileSystem);
        try {
            fileSystem.close();
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
    }
    
    private void uploadFile(final FileSystem fileSystem) {
        log.info("------------------------------->uploadFile");
        FSDataOutputStream out = null;
        try {
            out = fileSystem.create(new Path(FILE_PATH));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        FileInputStream in = null;
        try {
            in = new FileInputStream("/home/sky/hadoop.txt");
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try {
            IOUtils.copyBytes(in, out, 1024, true);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    @Override
    public void testStarted() {
    	super.testStarted();
    }
    
    @Override
    public void testEnded() {
    	super.testEnded();
    }
    
    public String printableFieldNamesToString() {
    	return this.printableFieldNamesToString(super.getSaveConfig());
    }
    
    /**
     * print file head list
     * @param c the configurations you selected
     * @return
     */
    public String printableFieldNamesToString(SampleSaveConfiguration c) {
    	return CSVSaveService.printableFieldNamesToString(c);
    }
    
//    public HdfsOperation() {
//        super();
//    }
//    
//    public String getNameNode() {
//		return this.getPropertyAsString(NAME_NODE);
//	}
//    public void setNameNode(String value) {
//    	this.setProperty(NAME_NODE, value);
//	}
//
//	public String getInputFilePath() {
//		return this.getPropertyAsString(INPUT_FILE_PATH);
//	}
//    public void setInputFilePath(String value) {
//    	this.setProperty(INPUT_FILE_PATH, value);
//	}
//    
//	public String getOutputFilePath() {
//		return this.getPropertyAsString(OUTPUT_FILE_PATH);
//	}
//    public void setOutputFilePath(String value) {
//    	this.setProperty(OUTPUT_FILE_PATH, value);
//	}
//    
//	public String getInterval() {
//		return this.getPropertyAsString(INTERVAL);
//	}
//    public void setInterval(String value) {
//    	this.setProperty(INTERVAL, value);
//	}
}
