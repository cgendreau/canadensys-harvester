package net.canadensys.processing.occurrence.task;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import net.canadensys.processing.ItemTaskIF;
import net.canadensys.processing.exception.TaskExecutionException;
import net.canadensys.processing.occurrence.SharedParameterEnum;
import net.canadensys.utils.ZipUtils;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.validator.routines.UrlValidator;
import org.apache.log4j.Logger;

/**
 * Task to prepare a Darwin Core Archive.
 * Preparation include : download (if necessary), unzip (if necessary)
 * @author canadensys
 *
 */
public class PrepareDwcaTask implements ItemTaskIF{
	//get log4j handler
	private static final Logger LOGGER = Logger.getLogger(PrepareDwcaTask.class);
	private static final String IPT_PREFIX = "dwca-";
	private static final String WORKING_FOLDER = "work";
	
	
	private String dwcaFilePath = null;
	
	/**
	 * @param sharedParameters out:SharedParameterEnum.DWCA_PATH
	 */
	@Override
	public void execute(Map<SharedParameterEnum,Object> sharedParameters){
		boolean dwcaFileExists = false;
		File dwcaFile = null;
		String dwcaIdentifier;
		
		//make sure the files exists
		if(dwcaFilePath != null){
			//Is this a URL?
			UrlValidator urlValidator = new UrlValidator();
			if(urlValidator.isValid(dwcaFilePath)){
				File workFolder = new File(WORKING_FOLDER);
				//make sure the folder exists
				workFolder.mkdir();
				URL dlUrl;
				try {
					dlUrl = new URL(dwcaFilePath);
					//Get the filename as defined by Content-Disposition:filename="dwca-mt-specimens.zip"
		        	String filename = dlUrl.openConnection().getHeaderField("Content-Disposition");
		            String destinationFile = workFolder.getAbsolutePath() +File.separator+ filename.replaceAll("\"", "").replace("filename=", "");
		            
		            downloadDwca(dlUrl, destinationFile);
		            dwcaFilePath  = destinationFile;
				} catch (MalformedURLException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			dwcaFile = new File(dwcaFilePath);
			dwcaFileExists = dwcaFile.exists();
		}
		if(!dwcaFileExists){
			throw new TaskExecutionException("Could not find the DarwinCore archive file "  + dwcaFilePath);
		}
		
		//set the unique identifier for this resource
		if(dwcaFile.isDirectory()){
			dwcaIdentifier = dwcaFile.getName();
		}
		else{
			dwcaIdentifier = FilenameUtils.getBaseName(dwcaFilePath);
		}
		
		
		//remove common IPT prefix
		if(StringUtils.startsWith(dwcaIdentifier, IPT_PREFIX)){
			dwcaIdentifier = StringUtils.removeStart(dwcaIdentifier, IPT_PREFIX);
		}
		
		if(FilenameUtils.isExtension(dwcaFilePath, "zip")){
			String unzippedFolder = FilenameUtils.removeExtension(dwcaFilePath);
			if(!ZipUtils.unzipFileOrFolder(new File(dwcaFilePath), unzippedFolder)){
				throw new TaskExecutionException("Error while unziping the DarwinCore Archive");
			}
			//use the unzipped folder
			dwcaFilePath = unzippedFolder;
		}
		
		//sanity check
		if(StringUtils.isBlank(dwcaIdentifier)){
			LOGGER.fatal("dwcaIdentifier cannot be empty");
			throw new TaskExecutionException("dwcaIdentifier cannot be empty");
		}
		
		// set shared variables
		sharedParameters.put(SharedParameterEnum.DWCA_PATH, dwcaFilePath);
	}
		
	private boolean downloadDwca(URL url, String destinationFile){
	 	File fl = null;
        OutputStream os = null;
        InputStream is = null;
        boolean success = false;
        try {
            fl = new File(destinationFile);
            os = new FileOutputStream(fl);
            is = url.openStream();

            //Download the file
            IOUtils.copy(is, os);
            
            success = true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (os != null) { 
                try {
					os.close();
				} catch (IOException e) {} 
            }
            if (is != null) { 
                try {
					is.close();
				} catch (IOException e) {} 
            }
        }
        return success;
	}

	public String getDwcaFilePath() {
		return dwcaFilePath;
	}

	/**
	 * Set the archive location. Should we use sharedParameters?
	 * @param dwcaFilePath
	 */
	public void setDwcaFilePath(String dwcaFilePath) {
		this.dwcaFilePath = dwcaFilePath;
	}

}
