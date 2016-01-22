package com.gics.edi.puma.task;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.TimerTask;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;

import com.gics.edi.common.PropManager;
import com.gics.edi.common.SFTPManager;
import com.gics.edi.common.XMLManager;
import com.gics.edi.conn.ConnectionManager;
import com.gics.edi.linker.Linker;

public class TaskPumaSendInboundTmp extends TimerTask {
	
	private static Logger loggerErr = Logger.getLogger("process.puma.err");
	private static Logger logger = Logger.getLogger("process.puma");
	
	public void run() {
		
		Linker linker = new Linker();
		PropManager propMgr = PropManager.getInstances();
		Properties prop = propMgr.getProp("sftpConf" , "puma");
		
		XMLManager xmlMgr = new XMLManager();
		Document doc = null;
		
		SFTPManager sftpMgr = null;
		FileInputStream isFile = null;
		FileOutputStream outTargetFile = null;
		
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		try {
			

			ArrayList<String[]> sendData = linker.createSendData("sendInbound1", "puma", "");
			StringBuilder sbXML = new StringBuilder();
			
			String curBrandNo="";
			for(int j=0; j<sendData.size(); j++) {
				if((!curBrandNo.equals(sendData.get(j)[16])&&curBrandNo.length()>0)) {
					sbXML.append(" </IDOC> ")
					.append(" </MBGMCR02> ");						
					doc = xmlMgr.createXmlDoc(sbXML.toString());
					xmlMgr.write(doc, prop.getProperty("send.upload.sourcePath")+"I_MBGMCR_"+sendData.get(j)[13]+".xml");						
					logger.info(sbXML.toString());
					
					sbXML.delete(0, sbXML.length());
				}
				
				if(!curBrandNo.equals(sendData.get(j)[16])) {
					sbXML.append( "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\" ?>")
					.append(" <MBGMCR02> ")
					.append(" <IDOC BEGIN=\"1\"> ")
					.append(" <EDI_DC40 SEGMENT=\"1\"> ")
					.append(" <TABNAM>").append(sendData.get(j)[0]).append("</TABNAM> ")
					.append(" <MANDT>").append(sendData.get(j)[1]).append("</MANDT> ")
					.append(" <DOCREL>").append(sendData.get(j)[2]).append("</DOCREL> ")
					.append(" <DIRECT>").append(sendData.get(j)[3]).append("</DIRECT> ")
					.append(" <IDOCTYP>").append(sendData.get(j)[4]).append("</IDOCTYP> ")
					.append(" <MESTYP>").append(sendData.get(j)[5]).append("</MESTYP> ")
					.append(" <STDMES>").append(sendData.get(j)[6]).append("</STDMES> ")
					.append(" <SNDPOR>").append(sendData.get(j)[7]).append("</SNDPOR> ")
					.append(" <SNDPRT>").append(sendData.get(j)[8]).append("</SNDPRT> ")
					.append(" <SNDPRN>").append(sendData.get(j)[9]).append("</SNDPRN> ")
					.append(" <RCVPOR>").append(sendData.get(j)[10]).append("</RCVPOR> ")
					.append(" <RCVPRT>").append(sendData.get(j)[11]).append("</RCVPRT> ")
					.append(" <RCVPRN>").append(sendData.get(j)[12]).append("</RCVPRN> ")
					.append(" <SNDLAD>").append(sendData.get(j)[13]).append("</SNDLAD> ")
					.append(" </EDI_DC40> ")
					.append(" <E1BP2017_GM_HEAD_01 SEGMENT=\"1\"> ")
					.append(" <PSTNG_DATE>").append(sendData.get(j)[14]).append("</PSTNG_DATE> ")
					.append(" <DOC_DATE>").append(sendData.get(j)[15]).append("</DOC_DATE> ")
					.append(" <REF_DOC_NO>").append(sendData.get(j)[16]).append("</REF_DOC_NO> ")
					.append(" <HEADER_TXT>").append(sendData.get(j)[17]).append("</HEADER_TXT> ")	
					.append(" </E1BP2017_GM_HEAD_01> ")
					.append(" <E1BP2017_GM_CODE SEGMENT=\"1\"> ")
					.append(" <GM_CODE>").append(sendData.get(j)[18]).append("</GM_CODE> ")
					.append(" </E1BP2017_GM_CODE> ")
					.append(" <E1BP2017_GM_ITEM_CREATE SEGMENT=\"1\"> ")
					.append(" <MOVE_TYPE>").append(sendData.get(j)[19]).append("</MOVE_TYPE> ")
					.append(" <ENTRY_QNT>").append(sendData.get(j)[20]).append("</ENTRY_QNT> ")
					.append(" <MVT_IND>").append(sendData.get(j)[21]).append("</MVT_IND> ")
					.append(" <DELIV_NUMB_TO_SEARCH>").append(sendData.get(j)[22]).append("</DELIV_NUMB_TO_SEARCH> ")
					.append(" <DELIV_ITEM_TO_SEARCH>").append(sendData.get(j)[23]).append("</DELIV_ITEM_TO_SEARCH> ")
					.append(" </E1BP2017_GM_ITEM_CREATE> ");
					curBrandNo = sendData.get(j)[16];
				} else {
					sbXML.append(" <E1BP2017_GM_ITEM_CREATE SEGMENT=\"1\"> ")
					.append(" <MOVE_TYPE>").append(sendData.get(j)[19]).append("</MOVE_TYPE> ")
					.append(" <ENTRY_QNT>").append(sendData.get(j)[20]).append("</ENTRY_QNT> ")
					.append(" <MVT_IND>").append(sendData.get(j)[21]).append("</MVT_IND> ")
					.append(" <DELIV_NUMB_TO_SEARCH>").append(sendData.get(j)[22]).append("</DELIV_NUMB_TO_SEARCH> ")
					.append(" <DELIV_ITEM_TO_SEARCH>").append(sendData.get(j)[23]).append("</DELIV_ITEM_TO_SEARCH> ")
					.append(" </E1BP2017_GM_ITEM_CREATE> ");
				}
				
				if((j+1) >= sendData.size()) {
					sbXML.append(" </IDOC> ")
					.append(" </MBGMCR02> ");						
					doc = xmlMgr.createXmlDoc(sbXML.toString());
					xmlMgr.write(doc, prop.getProperty("send.upload.sourcePath")+"I_MBGMCR_"+sendData.get(j)[13]+".xml");
					logger.info(sbXML.toString());
					sbXML.delete(0, sbXML.length());
					
					linker.updateSucc(sendData.get(j)[13], "E10");
				}
			}
		
			
			File sendPath = new File(prop.getProperty("send.upload.sourcePath"));
			File[] f = sendPath.listFiles();
			sftpMgr = new SFTPManager();
			
			byte[] buffer = new byte[1024];
			Properties fileProp = propMgr.getProp("fileInfo","puma");
			
			sftpMgr = new SFTPManager();
			sftpMgr.init(prop.getProperty("host")
						,prop.getProperty("id")
						,prop.getProperty("pw")
						,Integer.parseInt(prop.getProperty("port")));
			logger.info("init succ!");
			
			for(int i=0; i<f.length; i++) {
				sftpMgr.upload(prop.getProperty("send.upload.targetPath"), f[i]);
				
				outTargetFile = new FileOutputStream(fileProp.getProperty("send.backup.path")+f[i].getName());
				isFile = new FileInputStream(f[i]);
				if(f[i].getName().contains(".xml")) {
					int length=0;
					logger.info("isFile error: "+f[i].getName());
		    	    //copy the file content in bytes 
		    	    while ((length = isFile.read(buffer)) > 0){
		    	    	outTargetFile.write(buffer, 0, length);				 
		    	    }
		    	    
		    	    isFile.close();
		    	    outTargetFile.close();
		    	    f[i].delete();
				}
			}
			sftpMgr.disconnection();
		} catch(Exception e) {
			loggerErr.error(e.getMessage());
			loggerErr.error(e.toString());
		} finally {
			try {
				if(outTargetFile!=null) {
					outTargetFile.close();
				}
				if(isFile!=null) {
					isFile.close();
				}
				
				if(rs!=null) {
					rs.close();
				}
				
				if(pstmt!=null) {
					pstmt.close();
				}
				
				if(conn!=null) {
					conn.close();
				}
			} catch (Exception e) {
				loggerErr.error(e.getMessage());
				loggerErr.error(e.toString());
			}
		}
	}
}
