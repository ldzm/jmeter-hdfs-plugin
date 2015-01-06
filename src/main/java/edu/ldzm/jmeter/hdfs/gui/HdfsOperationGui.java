package edu.ldzm.jmeter.hdfs.gui;

import java.awt.BorderLayout;
import java.awt.GridLayout;

import javax.swing.Box;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

import org.apache.jmeter.samplers.Clearable;
import org.apache.jmeter.testelement.TestElement;
import org.apache.jmeter.visualizers.gui.AbstractListenerGui;

import edu.ldzm.jmeter.hdfs.HdfsOperation;

public class HdfsOperationGui extends AbstractListenerGui implements Clearable {

	private static final long serialVersionUID = 9150131463742788845L;

    private JTextField namenodTextField;
    private JTextField inputFilePathTextField;
    private JTextField outputFilePathTextField;
    private JTextField intervalTextField;
	
    public HdfsOperationGui() {
        super();
        init();
    }

    @Override
    public String getLabelResource() {
        return null;
    }

    @Override
	public String getStaticLabel() {
		return "HDFS Operation";
	}

    @Override
    public void configure(TestElement el) {
        super.configure(el);
       
        this.namenodTextField.setText(el.getPropertyAsString(HdfsOperation.NAME_NODE));
        this.inputFilePathTextField.setText(el.getPropertyAsString(HdfsOperation.INPUT_FILE_PATH));
        this.outputFilePathTextField.setText(el.getPropertyAsString(HdfsOperation.OUTPUT_FILE_PATH));
        this.intervalTextField.setText(el.getPropertyAsString(HdfsOperation.INTERVAL));
    }

    @Override
    public TestElement createTestElement() {
    	HdfsOperation hdfsOperation = new HdfsOperation();
        modifyTestElement(hdfsOperation);
        return hdfsOperation;
    }

    @Override
    public void modifyTestElement(TestElement c) {
        super.configureTestElement(c);
        c.setProperty(HdfsOperation.NAME_NODE, this.namenodTextField.getText());
        c.setProperty(HdfsOperation.INPUT_FILE_PATH, this.inputFilePathTextField.getText());
        c.setProperty(HdfsOperation.OUTPUT_FILE_PATH, this.outputFilePathTextField.getText());
        c.setProperty(HdfsOperation.INTERVAL, this.intervalTextField.getText());
    }

    private void init() {
        setLayout(new BorderLayout());
        setBorder(makeBorder());
        Box box = Box.createVerticalBox();
        box.add(makeTitlePanel());
        box.add(createPanel());      
        add(box, BorderLayout.NORTH);
    }
    
    private JPanel createPanel() {
    	JPanel panel = new JPanel(new GridLayout(8,2));
    	
    	JLabel namenodeLabel = new JLabel("Namenode"); 
    	namenodTextField = new JTextField(10);
    	namenodTextField.setName(HdfsOperation.NAME_NODE);
    	namenodeLabel.setLabelFor(namenodTextField);
    	panel.add(namenodeLabel);
    	panel.add(namenodTextField);
    	
    	JLabel inputFilePathLabel = new JLabel("Input File Path"); 
    	inputFilePathTextField = new JTextField(10);
    	inputFilePathTextField.setName(HdfsOperation.INPUT_FILE_PATH);
    	inputFilePathLabel.setLabelFor(inputFilePathTextField);
    	panel.add(inputFilePathLabel);
    	panel.add(inputFilePathTextField);
    	
    	JLabel outputFilePathLabel = new JLabel("Out File Path"); 
    	outputFilePathTextField = new JTextField(10);
    	outputFilePathTextField.setName(HdfsOperation.OUTPUT_FILE_PATH);
    	outputFilePathLabel.setLabelFor(outputFilePathTextField);
    	panel.add(outputFilePathLabel);
    	panel.add(outputFilePathTextField);
    	
    	JLabel intervalLabel = new JLabel("Interval Seconds"); 
    	intervalTextField = new JTextField(10);
    	intervalTextField.setName(HdfsOperation.INTERVAL);
    	intervalLabel.setLabelFor(intervalTextField);
    	panel.add(intervalLabel);
    	panel.add(intervalTextField);
    	
    	return panel;
    }

	@Override
	public void clearData() {
	}
}
