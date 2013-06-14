package net.canadensys.processing.occurrence.view;

import java.awt.Color;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ExecutionException;

import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSeparator;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;
import javax.swing.filechooser.FileFilter;

import net.canadensys.processing.ItemProgressListenerIF;
import net.canadensys.processing.occurrence.controller.StepControllerIF;
import net.canadensys.processing.occurrence.model.ApplicationStatus;
import net.canadensys.processing.occurrence.model.ApplicationStatus.JobStatusEnum;
import net.canadensys.processing.occurrence.model.IPTFeedModel;
import net.canadensys.processing.occurrence.model.ImportLogModel;
import net.canadensys.processing.occurrence.model.ResourceModel;

import org.apache.commons.io.FilenameUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class OccurrenceHarvesterMainView implements ItemProgressListenerIF,PropertyChangeListener{
	
	private JFrame batchProcessorFrame = null;
	
	private JPanel mainPanel = null;
	private JTextField pathToImportTxt = null;
	private ResourceModel resourceToImport = null;
	
	private JButton openFileBtn = null;
	private JButton openResourceBtn = null;
	
	private JButton importBtn = null;
	
	private JTextField bufferSchemaTxt = null;
	private JButton moveToPublicBtn = null;
	private ImageIcon loadingImg = null;
	private JLabel loadingLbl = null;
	private JTextArea statuxTxtArea = null;
	
	private JButton viewImportLogBtn = null;
	private JButton viewIPTFeedBtn = null;
	
	@Autowired
	private HarvesterViewModel harvesterViewModel;
	
	@Autowired
	@Qualifier(value="stepController")
	private StepControllerIF stepController;
	
	public void initView(){
		loadingImg = new ImageIcon(OccurrenceHarvesterMainView.class.getResource("/ajax-loader.gif"));
		
		batchProcessorFrame = new JFrame("Canadensys Harvester");
		batchProcessorFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		mainPanel = new JPanel();
		mainPanel.setLayout(new GridBagLayout());
		pathToImportTxt = new JTextField();
		pathToImportTxt.setColumns(30);
		pathToImportTxt.setEditable(false);
		openFileBtn = new JButton("Open File...");
		openResourceBtn = new JButton("Open Resource...");
		openFileBtn.setEnabled(false);
		openFileBtn.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				onChooseFile();
			}
		});
		
		openResourceBtn.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				onOpenResourceBtn();
			}
		});
		
		importBtn = new JButton("Import");
		importBtn.setToolTipText("Import the selected resource into the buffer schema");
		importBtn.setEnabled(false);
		importBtn.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				onImportFile();
			}
		});
		int lineIdx=0;
		GridBagConstraints c = new GridBagConstraints();
		c.gridx=0;
		c.gridy=lineIdx;
		c.anchor = GridBagConstraints.CENTER;
		c.gridwidth = 3;
		JLabel lbl = new JLabel("Working on database : " + harvesterViewModel.getDatabaseLocation());
		lbl.setForeground(Color.BLUE);
		mainPanel.add(lbl,c);
		
		//UI line break
		lineIdx++;
		c = new GridBagConstraints();
		c.gridx=0;
		c.gridy=lineIdx;
		c.anchor = GridBagConstraints.WEST;
		mainPanel.add(new JLabel("DwcA file to import:"),c);
		
		//UI line break
		lineIdx++;
		c = new GridBagConstraints();
		c.gridx=0;
		c.gridy=lineIdx;
		c.fill = GridBagConstraints.HORIZONTAL;
		c.weightx = 0.8;
		mainPanel.add(pathToImportTxt,c);
		
		c = new GridBagConstraints();
		c.gridx=1;
		c.gridy=lineIdx;
		mainPanel.add(openResourceBtn,c);
		
		c = new GridBagConstraints();
		c.gridx=2;
		c.gridy=lineIdx;
		mainPanel.add(openFileBtn,c);
		
		//UI line break
		lineIdx++;
		c = new GridBagConstraints();
		c.gridx=0;
		c.gridy=lineIdx;
		mainPanel.add(importBtn,c);
		
		//UI line break
		lineIdx++;
		c = new GridBagConstraints();
		c.gridx=0;
		c.gridy=lineIdx;
		c.gridwidth = 3;
		c.fill = GridBagConstraints.HORIZONTAL;
		mainPanel.add(new JSeparator(),c);
		
		//UI line break
		lineIdx++;
		c = new GridBagConstraints();
		c.gridx=0;
		c.gridy=lineIdx;
		c.gridwidth = 2;
		c.anchor = GridBagConstraints.WEST;
		mainPanel.add(new JLabel("DwcA ready to move:"),c);
		
		//UI line break
		lineIdx++;
		moveToPublicBtn = new JButton("Move");
		moveToPublicBtn.setToolTipText("Move to public schema");
		moveToPublicBtn.setEnabled(false);
		moveToPublicBtn.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				onMoveToPublic();
			}
		});
		c = new GridBagConstraints();
		c.gridx=2;
		c.gridy=lineIdx;
		c.anchor = GridBagConstraints.SOUTH;
		mainPanel.add(moveToPublicBtn ,c);
		
		bufferSchemaTxt = new JTextField();
		bufferSchemaTxt.setEnabled(false);
		c = new GridBagConstraints();
		c.gridx=0;
		c.gridy=lineIdx;
		c.fill = GridBagConstraints.BOTH;
		c.weightx = 0.8;
		c.weighty = 0.8;
		mainPanel.add(bufferSchemaTxt,c);
		
		//UI line break
		lineIdx++;
		c = new GridBagConstraints();
		c.gridx=0;
		c.gridy=lineIdx;
		c.gridwidth = 3;
		c.fill = GridBagConstraints.HORIZONTAL;
		mainPanel.add(new JSeparator(),c);
		
		//UI line break
		lineIdx++;
		c = new GridBagConstraints();
		c.gridx=0;
		c.gridy=lineIdx;
		c.anchor = GridBagConstraints.WEST;
		mainPanel.add(new JLabel("Current status : "),c);
		
		//UI line break
		lineIdx++;
		c = new GridBagConstraints();
		c.gridx=0;
		c.gridy=lineIdx;
		c.anchor = GridBagConstraints.WEST;
		loadingLbl = new JLabel("waiting",null, JLabel.CENTER);
		mainPanel.add(loadingLbl,c);
		
		//UI line break
		lineIdx++;
		c = new GridBagConstraints();
		c.gridx=0;
		c.gridy=lineIdx;
		c.gridwidth = 3;
		c.fill = GridBagConstraints.HORIZONTAL;
		mainPanel.add(new JSeparator(),c);
		
		//UI line break
		lineIdx++;
		c = new GridBagConstraints();
		c.gridx=0;
		c.gridy=lineIdx;
		c.gridwidth = 2;
		c.anchor = GridBagConstraints.WEST;
		mainPanel.add(new JLabel("Console:"),c);
		
		//UI line break
		lineIdx++;
		statuxTxtArea = new JTextArea();
		statuxTxtArea.setRows(15);
		c = new GridBagConstraints();
		c.gridx=0;
		c.gridy=lineIdx;
		c.gridwidth = 2;
		c.fill = GridBagConstraints.BOTH;
		c.weightx = 1.0;
		c.weighty = 1.0;
		mainPanel.add(new JScrollPane(statuxTxtArea),c);
		
		//UI line break
		lineIdx++;
		viewImportLogBtn = new JButton("View Import Log");
		viewImportLogBtn.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				onViewImportLog();
			}
		});
		c = new GridBagConstraints();
		c.gridx=0;
		c.gridy=lineIdx;
		c.anchor = GridBagConstraints.WEST;
		mainPanel.add(viewImportLogBtn,c);
		
		//UI line break
		lineIdx++;
		viewIPTFeedBtn = new JButton("View IPT RSS");
		viewIPTFeedBtn.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				onViewIPTFeed();
			}
		});
		c = new GridBagConstraints();
		c.gridx = 0;
		c.gridy = lineIdx;
		c.anchor = GridBagConstraints.WEST;
		mainPanel.add(viewIPTFeedBtn, c);
		
		//inner panel
		c = new GridBagConstraints();
		c.gridx=0;
		c.gridy=0;
		c.fill = GridBagConstraints.BOTH;
		c.weightx = 1.0;
		c.weighty = 1.0;
		
		batchProcessorFrame.setLayout(new GridBagLayout());
		batchProcessorFrame.add(mainPanel,c);
		batchProcessorFrame.pack();
		batchProcessorFrame.setLocationRelativeTo(null);
		batchProcessorFrame.setVisible(true);
		
		redirectSystemStreams();
		stepController.registerProgressListener(this);
		harvesterViewModel.addPropertyChangeListener(this);
	}
	
	private void onChooseFile(){
		JFileChooser fc = new JFileChooser();
		fc.setDialogTitle("DarwinCore archive to import");
		fc.setMultiSelectionEnabled(false);
		fc.setFileSelectionMode(JFileChooser.FILES_ONLY);
		fc.setFileFilter(new FileFilter() {
			@Override
			public String getDescription() {
				return "Darwin Core archive";
			}
			@Override
			public boolean accept(File file) {
				if(FilenameUtils.getExtension(file.getName()).equalsIgnoreCase("zip")){
					return true;
				}
				return false;
			}
		});
		int returnVal = fc.showOpenDialog(batchProcessorFrame);
		if(returnVal == JFileChooser.APPROVE_OPTION){
			pathToImportTxt.setText(fc.getSelectedFile().getAbsolutePath());
			importBtn.setEnabled(true);
		}
	}
	
	private void onOpenResourceBtn(){
		ResourceChooser urlChooser = new ResourceChooser(stepController.getResourceModelList());
		urlChooser.setLocationRelativeTo(null);
		urlChooser.setVisible(true);
		
		ResourceModel selectedResource = urlChooser.getSelectedResource();
		if(selectedResource != null){
			pathToImportTxt.setText(selectedResource.getName());
			importBtn.setEnabled(true);
		}
		resourceToImport = selectedResource;
	}
	
	private void onImportFile(){
		openResourceBtn.setEnabled(false);
		importBtn.setEnabled(false);
		moveToPublicBtn.setEnabled(false);
		loadingLbl.setIcon(loadingImg);
		final SwingWorker<String,Object> swingWorker = new SwingWorker<String, Object>() {
		       @Override
		       public String doInBackground() {
		    	   stepController.importDwcA(resourceToImport.getResource_id());
		    	   return resourceToImport.getSource_file_id();
		       }

		       @Override
		       protected void done() {
		       }
		};
		swingWorker.execute();
	}
	
	private void onMoveToPublic(){
		moveToPublicBtn.setEnabled(false);
		loadingLbl.setIcon(loadingImg);
		
		final SwingWorker<Boolean,Object> swingWorker = new SwingWorker<Boolean, Object>() {
		       @Override
		       public Boolean doInBackground() {
					stepController.moveToPublicSchema(bufferSchemaTxt.getText());
					return true;
		       }

		       @Override
		       protected void done() {
		    	   try {
					if(get()){
						   onMoveDone(JobStatusEnum.DONE_SUCCESS);
					   }
					   else{
						   onMoveDone(JobStatusEnum.DONE_ERROR);
					   }
				} catch (InterruptedException e) {
					 onMoveDone(JobStatusEnum.DONE_ERROR);
				} catch (ExecutionException e) {
					 onMoveDone(JobStatusEnum.DONE_ERROR);
				}
		      }
		};
		swingWorker.execute();
	}
	
	private void onViewImportLog(){
		Vector<String> headers = new Vector<String>();
		headers.add("SourceFileId");
		headers.add("Record Quantity");
		headers.add("Updated By");
		headers.add("Event Date");
		
		ImportLogDialog dlg = new ImportLogDialog(headers);
		
		List<ImportLogModel> importLogModelList = stepController.getSortedImportLogModelList();
		dlg.loadData(importLogModelList);
		dlg.setLocationRelativeTo(null);
		dlg.setVisible(true);
	}
	
	private void onViewIPTFeed() {
		Vector<String> headers = new Vector<String>();
		headers.add("Title");
		headers.add("Resource URL");
		headers.add("Key");
		headers.add("Publication date");

		IPTFeedDialog dlg = new IPTFeedDialog(headers);

		List<IPTFeedModel> importLogModelList = stepController.getIPTFeed();
		dlg.loadData(importLogModelList);
		dlg.setLocationRelativeTo(null);
		dlg.setVisible(true);
	}
	
	private void onImportDone(JobStatusEnum status, String datasetShortName){
		loadingLbl.setIcon(null);
		 try {
      	   if(JobStatusEnum.DONE_SUCCESS.equals(status)){
      		   bufferSchemaTxt.setText(datasetShortName);
      		   moveToPublicBtn.setEnabled(true);
      		   
      		   SwingUtilities.invokeLater(new Runnable() {
      			    public void run() {
      			    	loadingLbl.setText("Import done, ready to move");
      			    }
      		   });
      	   }
      	   else{
      		   JOptionPane.showMessageDialog(batchProcessorFrame, "Something went wrong, details in the console.", "Error", JOptionPane.ERROR_MESSAGE);
      		   SwingUtilities.invokeLater(new Runnable() {
     			    public void run() {
     			    	loadingLbl.setText("Error occurred while importing");
     			    }
     		   });
      	   }
         } catch (Exception e) {
      	   e.printStackTrace();
      	   JOptionPane.showMessageDialog(batchProcessorFrame, "Something went wrong, details in the console.", "Error", JOptionPane.ERROR_MESSAGE);
  		   SwingUtilities.invokeLater(new Runnable() {
			    public void run() {
			    	loadingLbl.setText("Error occurred while importing");
			    }
		   });
         }
	}
	
	private void onMoveDone(JobStatusEnum status){
		loadingLbl.setIcon(null);
		if(JobStatusEnum.DONE_SUCCESS.equals(status)){
			JOptionPane.showMessageDialog(batchProcessorFrame, "Everything seems fine.", "Info", JOptionPane.INFORMATION_MESSAGE);
			bufferSchemaTxt.setText("");
			pathToImportTxt.setText("");
			loadingLbl.setText("Move done");
		}
		else{
			JOptionPane.showMessageDialog(batchProcessorFrame, "Something went wrong, details in the console.", "Error", JOptionPane.ERROR_MESSAGE);
			loadingLbl.setText("Error occurred while moving");
		}
	}
	
	private void updateTextArea(final String text) {
		SwingUtilities.invokeLater(new Runnable() {
		    public void run() {
		    	statuxTxtArea.append(text);
		    }
		  });
	}
		 
	private void redirectSystemStreams() {
		  OutputStream out = new OutputStream() {
		    @Override
		    public void write(int b) throws IOException {
		      updateTextArea(String.valueOf((char) b));
		    }
		 
		    @Override
		    public void write(byte[] b, int off, int len) throws IOException {
		      updateTextArea(new String(b, off, len));
		    }
		 
		    @Override
		    public void write(byte[] b) throws IOException {
		      write(b, 0, b.length);
		    }
		  };
		 
		  System.setOut(new PrintStream(out, true));
		  System.setErr(new PrintStream(out, true));
	}

	@Override
	public void onProgress(final int current, final int total) {
		SwingUtilities.invokeLater(new Runnable() {
		    public void run() {
		    	loadingLbl.setText(current + "/" + total);
		    }
		  });
	}

	@Override
	public void propertyChange(PropertyChangeEvent evt) {
		if("applicationStatus".equals(evt.getPropertyName())){
			onImportDone(((ApplicationStatus)evt.getNewValue()).getImportStatus(),resourceToImport.getSource_file_id());
		}
		
	}
}
