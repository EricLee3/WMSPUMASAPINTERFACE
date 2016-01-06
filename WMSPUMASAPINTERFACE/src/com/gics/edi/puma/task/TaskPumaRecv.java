package com.gics.edi.puma.task;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.TimerTask;
import org.apache.log4j.Logger;

import com.gics.edi.common.PropManager;
import com.gics.edi.common.SFTPManager;
import com.gics.edi.linker.Linker;

public class TaskPumaRecv extends TimerTask {
	private static Logger loggerErr = Logger.getLogger("process.puma.err");
	private static Logger logger = Logger.getLogger("process.puma");
	
	public void run() {
		logger.info("PumaRecvLinkerMain Start! ");
		PropManager propMgr = PropManager.getInstances();
		Properties prop = propMgr.getProp("sftpConf" , "puma");
		Properties fileProp = propMgr.getProp("fileInfo","puma");
		SFTPManager sftpMgr = null;
		OutputStream os = null;
		OutputStream outTargetFile = null;
		InputStream isFile = null;
		try {
			int cnt = 1;
			sftpMgr = new SFTPManager();
			sftpMgr.init(prop.getProperty("host")
					,prop.getProperty("id")
					,prop.getProperty("pw")
					,Integer.parseInt(prop.getProperty("port")));
			logger.info("init succ!");
			ArrayList<String> sourceFiles = new ArrayList<String>();
			String[] fileAttr = prop.getProperty("recv.sourcePath.attr").split(",");
			String[] backExcept = fileProp.getProperty("backup.except").split(",");
			
			for(int i=0; i<fileAttr.length; i++) {
				logger.info(prop.getProperty("recv.sourcePath")+fileAttr[i]+"/");
				ArrayList<String> lsFiles = sftpMgr.ls(prop.getProperty("recv.sourcePath")+fileAttr[i]+"/");

				logger.info("ls succ!");
				for(int j=0; j<lsFiles.size(); j++) {
					cnt=1;
					if(lsFiles.get(j).contains(".xml")) {
						while(fileProp.getProperty("file.list"+cnt)!=null) {
							String fileName = "";
							if(lsFiles.get(j).contains(fileProp.getProperty("file.list"+cnt))) {
								fileName = lsFiles.get(j).trim().substring(lsFiles.get(j)
											.indexOf(fileProp.getProperty("file.list"+cnt)), lsFiles.get(j).length());

							} 

							if(fileName.length() > 0) {
								if(sourceFiles.size()>0) {
									if(!fileName.equals(sourceFiles.get(sourceFiles.size()-1))){
										sourceFiles.add(fileName);
//									break;
									}
								} else {
									sourceFiles.add(fileName);
								}
							}
							cnt++;
						}
					}
				}

				Collections.sort(sourceFiles);
//				os = new FileOutputStream(new File(fileProp.getProperty("path")+"fileInfo.properties"));
				cnt=1;
				while(fileProp.getProperty("file.list"+cnt)!=null) {
					int j=0;
					while(sourceFiles.size() > 0 && sourceFiles.size() > j) {
						if(sourceFiles.get(j).contains(fileProp.getProperty("file.list"+cnt))) {
							boolean isEx=false;
							logger.info("download start - "+sourceFiles.get(j));

							sftpMgr.download(prop.getProperty("recv.sourcePath")+fileAttr[i]+"/"
									,sourceFiles.get(j)
									, prop.getProperty("recv.targetPath")+sourceFiles.get(j));
							String num = String.valueOf(Integer.parseInt(sourceFiles.get(j)
												.substring(sourceFiles.get(j).lastIndexOf("_")+1, 
												sourceFiles.get(j).length()).replaceAll(".xml", "")));

							logger.info("download done - "+fileProp.getProperty("destFile.type"+cnt)+"_"+num);
//							fileProp.setProperty("file.lastFile"+cnt, num);

							for(int ex=0; ex<backExcept.length; ex++) {
								if(cnt==Integer.parseInt(backExcept[ex])) {
									isEx = true;
									break;
								}
							}

							if(!isEx) {
								sftpMgr.deleteFile(prop.getProperty("recv.sourcePath")+fileAttr[i]+"/", sourceFiles.get(j));
								logger.info("파일삭제");
								sourceFiles.remove(j);
							} else {
								j++;
							}
						} else {
							j++;
						}
					}
					cnt++;
				}
//				fileProp.store(os, "");
//				os.flush();
			}

			sftpMgr.disconnection();
			

			File recvPath = new File(prop.getProperty("recv.targetPath"));
			String[] recvFiles = recvPath.list();
			Arrays.sort(recvFiles);
			byte[] buffer = new byte[1024];
			Linker linker = new Linker();
			cnt=1;
			while(fileProp.getProperty("file.list"+cnt)!=null) {
				logger.debug(fileProp.getProperty("file.list"+cnt));
				for(int i=0; i<recvFiles.length; i++) {
					String result = "";
					boolean isDone = false;
					if(recvFiles[i] == null) {
						continue;
					}

					isFile = new FileInputStream(new File(prop.getProperty("recv.targetPath")+recvFiles[i]));
					logger.info(fileProp.getProperty("file.list"+cnt));
					logger.info("recvFiles["+i+"]: "+recvFiles[i]);
					if(recvFiles[i].contains(".xml")) {
						if(recvFiles[i].contains(fileProp.getProperty("file.list"+cnt))) {
							result = linker.callWMSProc(fileProp.getProperty("destFile.type"+cnt), 
									prop.getProperty("recv.targetPath")+recvFiles[i]);
							logger.info("result: "+result);
							if(result.trim().equals("F")) {
								continue;
							} else {
								outTargetFile = new FileOutputStream(fileProp.getProperty("recv.backup.path")+recvFiles[i]);
								int length=0;
								logger.info("callWMS recvFile: "+recvFiles[i]);

								while ((length = isFile.read(buffer)) > 0){
									outTargetFile.write(buffer, 0, length);				 
								}
    	    
								isFile.close();
								outTargetFile.close();
    	    
								isDone = true;
							}
						}
					}

					boolean isEx=false;
					for(int j=0; j<backExcept.length; j++) {
						if(cnt==Integer.parseInt(backExcept[j])) {
							isEx=true;
							break;
						}
					}
					
					logger.debug(recvFiles[i]);
					logger.debug("isDone:: "+isDone);
					logger.debug("isEx:: "+isEx);
					
					if(cnt==6) {
						if(isDone) {
							new File(prop.getProperty("recv.targetPath")+recvFiles[i]).delete();
							recvFiles[i] = null;
						}
					} else {
						if(!isEx && isDone) {
							new File(prop.getProperty("recv.targetPath")+recvFiles[i]).delete();
							recvFiles[i] = null;
						}
					}
					

				}
				cnt++;
			}

			logger.info("PumaRecvLinkerMain finish! ");
		} catch (IOException e) {
			loggerErr.error(e.getMessage());
		} catch (Exception e) {
			loggerErr.error(e.getMessage());
		} finally {
			try {
				if(os!=null) {
					os.close();
				}
			} catch (Exception e) {
				loggerErr.error(e.getMessage());
			}
		}
	}
}
