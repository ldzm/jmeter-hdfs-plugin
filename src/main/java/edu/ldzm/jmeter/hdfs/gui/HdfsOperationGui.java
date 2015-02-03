package edu.ldzm.jmeter.hdfs.gui;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JFileChooser;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.table.TableCellRenderer;

import org.apache.jmeter.gui.util.FileDialoger;
import org.apache.jmeter.gui.util.HeaderAsPropertyRenderer;
import org.apache.jmeter.samplers.Clearable;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.save.CSVSaveService;
import org.apache.jmeter.testelement.TestElement;
import org.apache.jmeter.util.Calculator;
import org.apache.jmeter.util.JMeterUtils;
import org.apache.jmeter.visualizers.gui.AbstractVisualizer;
import org.apache.jorphan.gui.NumberRenderer;
import org.apache.jorphan.gui.ObjectTableModel;
import org.apache.jorphan.gui.RateRenderer;
import org.apache.jorphan.gui.RendererUtils;
import org.apache.jorphan.reflect.Functor;
import org.apache.jorphan.util.JOrphanUtils;

import edu.ldzm.jmeter.hdfs.HdfsOperation;

public class HdfsOperationGui extends AbstractVisualizer implements Clearable, ActionListener {

	private static final long serialVersionUID = 9150131463742788845L;

    private JTextField namenodTextField;
    private JTextField inputFilePathTextField;
    private JTextField outputFilePathTextField;
    private JTextField intervalTextField;
    
    private JTextField hadoopCmdPathTextField;
    private JTextField jobPathTextField;
    private JTextField exeHadoopTaskIntervalTextField;
	
    private static final String USE_GROUP_NAME = "useGroupName"; //$NON-NLS-1$

    private static final String SAVE_HEADERS   = "saveHeaders"; //$NON-NLS-1$

    private static final String[] COLUMNS = {
            "sampler_label",               //$NON-NLS-1$
            "aggregate_report_count",      //$NON-NLS-1$
            "average",                     //$NON-NLS-1$
            "aggregate_report_min",        //$NON-NLS-1$
            "aggregate_report_max",        //$NON-NLS-1$
            "aggregate_report_stddev",     //$NON-NLS-1$
            "aggregate_report_error%",     //$NON-NLS-1$
            "aggregate_report_rate",       //$NON-NLS-1$
            "aggregate_report_bandwidth",  //$NON-NLS-1$
            "average_bytes",               //$NON-NLS-1$
            };

    private final String TOTAL_ROW_LABEL
        = JMeterUtils.getResString("aggregate_report_total_label");  //$NON-NLS-1$

    private JTable myJTable;

    private JScrollPane myScrollPane;

    private final JButton saveTable =
        new JButton(JMeterUtils.getResString("aggregate_graph_save_table"));            //$NON-NLS-1$

    private final JCheckBox saveHeaders = // should header be saved with the data?
        new JCheckBox(JMeterUtils.getResString("aggregate_graph_save_table_header"),true);    //$NON-NLS-1$

    private final JCheckBox useGroupName =
        new JCheckBox(JMeterUtils.getResString("aggregate_graph_use_group_name"));            //$NON-NLS-1$

    private transient ObjectTableModel model;

    /**
     * Lock used to protect tableRows update + model update
     */
    private final transient Object lock = new Object();

    private final Map<String, Calculator> tableRows =
        new ConcurrentHashMap<String, Calculator>();

    // Column renderers
    private static final TableCellRenderer[] RENDERERS =
        new TableCellRenderer[]{
            null, // Label
            null, // count
            null, // Mean
            null, // Min
            null, // Max
            new NumberRenderer("#0.00"), // Std Dev.
            new NumberRenderer("#0.00%"), // Error %age
            new RateRenderer("#.0"),      // Throughput
            new NumberRenderer("#0.00"),  // kB/sec
            new NumberRenderer("#.0"),    // avg. pageSize
        };

    
    public HdfsOperationGui() {
        super();
        model = new ObjectTableModel(COLUMNS,
                Calculator.class,// All rows have this class
                new Functor[] {
                    new Functor("getLabel"),              //$NON-NLS-1$
                    new Functor("getCount"),              //$NON-NLS-1$
                    new Functor("getMeanAsNumber"),       //$NON-NLS-1$
                    new Functor("getMin"),                //$NON-NLS-1$
                    new Functor("getMax"),                //$NON-NLS-1$
                    new Functor("getStandardDeviation"),  //$NON-NLS-1$
                    new Functor("getErrorPercentage"),    //$NON-NLS-1$
                    new Functor("getRate"),               //$NON-NLS-1$
                    new Functor("getKBPerSecond"),        //$NON-NLS-1$
                    new Functor("getAvgPageBytes"),       //$NON-NLS-1$
                },
                new Functor[] { null, null, null, null, null, null, null, null , null, null },
                new Class[] { String.class, Long.class, Long.class, Long.class, Long.class,
                              String.class, String.class, String.class, String.class, String.class });
        clearData();
        namenodTextField = new JTextField(10);
        inputFilePathTextField = new JTextField(10);
        outputFilePathTextField = new JTextField(10);
        intervalTextField = new JTextField(10);
        
        hadoopCmdPathTextField = new JTextField(10);
        jobPathTextField = new JTextField(10);
        exeHadoopTaskIntervalTextField = new JTextField(10);
        init();
    }

    /** @deprecated - only for use in testing */
    @Deprecated
    public static boolean testFunctors(){
    	HdfsOperationGui instance = new HdfsOperationGui();
        return instance.model.checkFunctors(null,instance.getClass());
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
    public void add(final SampleResult res) {
        final String sampleLabel = res.getSampleLabel(useGroupName.isSelected());
        JMeterUtils.runSafe(new Runnable() {
            @Override
            public void run() {
                Calculator row = null;
                synchronized (lock) {
                    row = tableRows.get(sampleLabel);
                    if (row == null) {
                        row = new Calculator(sampleLabel);
                        tableRows.put(row.getLabel(), row);
                        model.insertRow(row, model.getRowCount() - 1);
                    }
                }
                /*
                 * Synch is needed because multiple threads can update the counts.
                 */
                synchronized(row) {
                    row.addSample(res);
                }
                Calculator tot = tableRows.get(TOTAL_ROW_LABEL);
                synchronized(tot) {
                    tot.addSample(res);
                }
                model.fireTableDataChanged();                
            }
        });
    }
    
    /**
     * Clears this visualizer and its model, and forces a repaint of the table.
     */
    @Override
    public void clearData() {
        //Synch is needed because a clear can occur while add occurs
        synchronized (lock) {
            model.clearData();
            tableRows.clear();
            tableRows.put(TOTAL_ROW_LABEL, new Calculator(TOTAL_ROW_LABEL));
            model.addRow(tableRows.get(TOTAL_ROW_LABEL));
        }
    }

    @Override
    public void configure(TestElement el) {
        super.configure(el);
        useGroupName.setSelected(el.getPropertyAsBoolean(USE_GROUP_NAME, false));
        saveHeaders.setSelected(el.getPropertyAsBoolean(SAVE_HEADERS, true));
        this.namenodTextField.setText(el.getPropertyAsString(HdfsOperation.NAME_NODE));
        this.inputFilePathTextField.setText(el.getPropertyAsString(HdfsOperation.INPUT_FILE_PATH));
        this.outputFilePathTextField.setText(el.getPropertyAsString(HdfsOperation.OUTPUT_FILE_PATH));
        this.intervalTextField.setText(el.getPropertyAsString(HdfsOperation.INTERVAL));
        
        this.hadoopCmdPathTextField.setText(el.getPropertyAsString(HdfsOperation.HADOOP_CMD_PATH));
        this.jobPathTextField.setText(el.getPropertyAsString(HdfsOperation.JOB_PATH));
        this.exeHadoopTaskIntervalTextField.setText(el.getPropertyAsString(HdfsOperation.EXE_HADOOP_TASK_INTERVAL));
    }

    @Override
    public TestElement createTestElement() {

    	HdfsOperation operation = new HdfsOperation();
    	modifyTestElement(operation);
    	return operation;
    }
    
    @Override
    public void modifyTestElement(TestElement c) {
        super.modifyTestElement(c);
        c.setProperty(USE_GROUP_NAME, useGroupName.isSelected(), false);
        c.setProperty(SAVE_HEADERS, saveHeaders.isSelected(), true);
        String naString = HdfsOperation.NAME_NODE;
        String tString = this.namenodTextField.getText();
        c.setProperty(naString, tString);
        c.setProperty(HdfsOperation.NAME_NODE, this.namenodTextField.getText());
        c.setProperty(HdfsOperation.INPUT_FILE_PATH, this.inputFilePathTextField.getText());
        c.setProperty(HdfsOperation.OUTPUT_FILE_PATH, this.outputFilePathTextField.getText());
        c.setProperty(HdfsOperation.INTERVAL, this.intervalTextField.getText());

        c.setProperty(HdfsOperation.HADOOP_CMD_PATH, this.hadoopCmdPathTextField.getText());
        c.setProperty(HdfsOperation.JOB_PATH, this.jobPathTextField.getText());
        c.setProperty(HdfsOperation.EXE_HADOOP_TASK_INTERVAL, this.exeHadoopTaskIntervalTextField.getText());
    }

    private void init() {
        setLayout(new BorderLayout());
        setBorder(makeBorder());
        
        Box box = Box.createVerticalBox();
        box.add(makeTitlePanel());
        
        Box paramBox = Box.createVerticalBox();
        paramBox.setBorder(BorderFactory.createTitledBorder("Put File to HDFS"));
        paramBox.add(createPanel(new JLabel("Namenode:"), HdfsOperation.NAME_NODE, namenodTextField));
        paramBox.add(createPanel(new JLabel("Input File Path:"), HdfsOperation.INPUT_FILE_PATH, inputFilePathTextField));
        paramBox.add(createPanel(new JLabel("Out File Path:"), HdfsOperation.OUTPUT_FILE_PATH, outputFilePathTextField));
        paramBox.add(createPanel(new JLabel("Interval(s):"), HdfsOperation.INTERVAL, intervalTextField));
        
        Box hadoopParamBox = Box.createVerticalBox();
        hadoopParamBox.setBorder(BorderFactory.createTitledBorder("Hadoop CMD"));
        hadoopParamBox.add(createPanel(new JLabel("Hadoop Cmd Path:"), HdfsOperation.HADOOP_CMD_PATH, hadoopCmdPathTextField));
        hadoopParamBox.add(createPanel(new JLabel("Job Path:"), HdfsOperation.JOB_PATH, jobPathTextField));
        hadoopParamBox.add(createPanel(new JLabel("Execute Hadoop Task Interval(s)"), HdfsOperation.EXE_HADOOP_TASK_INTERVAL, exeHadoopTaskIntervalTextField));
        
        box.add(paramBox);
        box.add(hadoopParamBox);
        this.add(box, BorderLayout.NORTH);
        
        myJTable = new JTable(model);
        myJTable.getTableHeader().setDefaultRenderer(new HeaderAsPropertyRenderer());
        myJTable.setPreferredScrollableViewportSize(new Dimension(500, 70));
        RendererUtils.applyRenderers(myJTable, RENDERERS);
        myScrollPane = new JScrollPane(myJTable);
        saveTable.addActionListener(this);
        this.add(myScrollPane, BorderLayout.CENTER);
        
        JPanel opts = new JPanel();
        opts.add(useGroupName, BorderLayout.WEST);
        opts.add(saveTable, BorderLayout.CENTER);
        opts.add(saveHeaders, BorderLayout.EAST);
        this.add(opts,BorderLayout.SOUTH);
    }
    
    private JPanel createPanel(JLabel label, String textFieldName, JTextField textField) {

    	textField.setName(textFieldName);
    	label.setLabelFor(textField);

    	JPanel panel = new JPanel(new BorderLayout(5, 0));
    	panel.add(label, BorderLayout.WEST);
    	panel.add(textField, BorderLayout.CENTER);
    	
        return panel;
    }
    @Override
    public void actionPerformed(ActionEvent ev) {
        if (ev.getSource() == saveTable) {
            JFileChooser chooser = FileDialoger.promptToSaveFile("summary.csv");//$NON-NLS-1$
            if (chooser == null) {
                return;
            }
            FileWriter writer = null;
            try {
                writer = new FileWriter(chooser.getSelectedFile());
                CSVSaveService.saveCSVStats(model,writer, saveHeaders.isSelected());
            } catch (FileNotFoundException e) {
                JMeterUtils.reportErrorToUser(e.getMessage(), "Error saving data");
            } catch (IOException e) {
                JMeterUtils.reportErrorToUser(e.getMessage(), "Error saving data");
            } finally {
                JOrphanUtils.closeQuietly(writer);
            }
        }
    }
}
