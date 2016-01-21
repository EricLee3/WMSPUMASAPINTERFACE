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

public class TaskPumaSendOutbound extends TimerTask {
	private static Logger loggerErr = Logger.getLogger("process.puma.err");
	private static Logger logger = Logger.getLogger("process.puma");
	
	public void run() {
		Linker linker = new Linker();
		StringBuilder sbQuery = new StringBuilder();
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
		
		sbQuery.append(" SELECT OUTBOUND_NO ")
		.append(" FROM LO020NM ")
		.append(" WHERE CENTER_CD = 'A1' ") 
		.append(" AND OUTBOUND_STATE = '50' ") 
		.append(" AND ( SEND_STATE = '00' OR SEND_STATE IS NULL ) ")
//		.append(" AND ( FREE_VAL5 = '00' OR FREE_VAL5 IS NULL ) ")
		.append(" AND BRAND_CD = '6101' ") 
		.append(" AND OUTBOUND_DATE <= TO_DATE(TO_CHAR(SYSDATE,'YYYYMMDD'),'YYYYMMDD') ")
		.append(" AND  DELIVERY_CD <> 'Z999' ");
		logger.debug(sbQuery.toString());
		
		try {
			ConnectionManager connMgr = ConnectionManager.getInstances();
			conn = connMgr.connect("wmsdb");
			
			pstmt = conn.prepareStatement(sbQuery.toString());
			rs = pstmt.executeQuery();
			
			ArrayList<String> outboundList = new ArrayList<String>();
			while(rs.next()) {
				outboundList.add(rs.getString(1));
			}
			if(outboundList.size()<1) {
				logger.info("send data is nothing !");
			}
			
			for(int i=0; i<outboundList.size(); i++) {
				ArrayList<String[]> sendData = linker.createSendData("sendOutbound", "puma", outboundList.get(i));
				StringBuilder sbXML = new StringBuilder();
			
				String curBrandNo="";
				for(int j=0; j<sendData.size(); j++) {
					if((!curBrandNo.equals(sendData.get(j)[13])&&curBrandNo.length()>0)) {
						sbXML.append(" <E1BPEXTC SEGMENT=\"1\"><FIELD1>")
						.append(sendData.get(j)[12])
						.append("</FIELD1></E1BPEXTC> ")
						.append(" </E1SHP_OBDLV_CONFIRM_DECENTR> ")
						.append(" </IDOC> ")
						.append(" </SHP_OBDLV_CONFIRM_DECENTRAL02> ");						
						doc = xmlMgr.createXmlDoc(sbXML.toString());
						xmlMgr.write(doc, prop.getProperty("send.upload.sourcePath")+"I_SHP_OB_"+sendData.get(j)[12]+".xml");
						logger.info(sbXML.toString());
						
						sbXML.delete(0, sbXML.length());
					}
					if(!curBrandNo.equals(sendData.get(j)[13])) {
						curBrandNo = sendData.get(j)[13];
						sbXML.append( "<?xml version=\"1.0\" encoding=\"UTF-8\"?>")
						.append(" <SHP_OBDLV_CONFIRM_DECENTRAL02> ")
						.append(" <IDOC BEGIN=\"1\"> ")
						.append(" <EDI_DC40 SEGMENT=\"1\"> ")
						.append(" <TABNAM>").append(sendData.get(j)[0]).append("</TABNAM> ")
						.append(" <DOCREL>").append(sendData.get(j)[1]).append("</DOCREL> ")
						.append(" <DIRECT>").append(sendData.get(j)[2]).append("</DIRECT> ")
						.append(" <IDOCTYP>").append(sendData.get(j)[3]).append("</IDOCTYP> ")
						.append(" <MESTYP>").append(sendData.get(j)[4]).append("</MESTYP> ")
						.append(" <STDMES>").append(sendData.get(j)[5]).append("</STDMES> ")
						.append(" <SNDPOR>").append(sendData.get(j)[6]).append("</SNDPOR> ")
						.append(" <SNDPRT>").append(sendData.get(j)[7]).append("</SNDPRT> ")
						.append(" <SNDPRN>").append(sendData.get(j)[8]).append("</SNDPRN> ")
						.append(" <RCVPOR>").append(sendData.get(j)[9]).append("</RCVPOR> ")
						.append(" <RCVPRT>").append(sendData.get(j)[10]).append("</RCVPRT> ")
						.append(" <RCVPRN>").append(sendData.get(j)[11]).append("</RCVPRN> ")
						.append(" <SNDLAD>").append(sendData.get(j)[12]).append("</SNDLAD> ")
						.append(" </EDI_DC40> ")
						.append(" <E1SHP_OBDLV_CONFIRM_DECENTR SEGMENT=\"1\">")
						.append(" <DELIVERY>").append(sendData.get(j)[13]).append("</DELIVERY> ")
						.append(" <E1BPOBDLVHDRCON SEGMENT=\"1\"> ")
						.append(" <DELIV_NUMB>").append(sendData.get(j)[14]).append("</DELIV_NUMB> ")
						.append(" </E1BPOBDLVHDRCON> ")
						.append(" <E1BPOBDLVHDRCTRLCON SEGMENT=\"1\">")
						.append(" <DELIV_NUMB>").append(sendData.get(j)[15]).append("</DELIV_NUMB> ")
						.append(" <POST_GI_FLG>").append(sendData.get(j)[16]).append("</POST_GI_FLG> ")
						.append(" </E1BPOBDLVHDRCTRLCON> ")
						.append(" <E1BPDLVDEADLN SEGMENT=\"1\">")
						.append(" <DELIV_NUMB>").append(sendData.get(j)[17]).append("</DELIV_NUMB>")
						.append(" <TIMETYPE>").append(sendData.get(j)[18]).append("</TIMETYPE>")
						.append(" <TIMESTAMP_UTC>").append(sendData.get(j)[19]).append("</TIMESTAMP_UTC>")
						.append(" </E1BPDLVDEADLN> ");
						for(int k=j; k<sendData.size(); k++ ) {
							if(curBrandNo.equals(sendData.get(k)[13])) {
								sbXML.append(" <E1BPOBDLVITEMCON SEGMENT=\"1\">")
								.append(" <DELIV_NUMB>").append(sendData.get(k)[20]).append("</DELIV_NUMB> ")
								.append(" <DELIV_ITEM>").append(sendData.get(k)[21]).append("</DELIV_ITEM> ")
								.append(" <DLV_QTY>").append(sendData.get(k)[22]).append("</DLV_QTY> ")
								.append(" <DLV_QTY_IMUNIT>").append(sendData.get(k)[23]).append("</DLV_QTY_IMUNIT> ")
								.append(" <FACT_UNIT_NOM>").append(sendData.get(k)[24]).append("</FACT_UNIT_NOM> ")
								.append(" <FACT_UNIT_DENOM>").append(sendData.get(k)[25]).append("</FACT_UNIT_DENOM> ")
								.append(" </E1BPOBDLVITEMCON> ");
							} else {
								break;
							}
						}
						
						for(int k=j; k<sendData.size(); k++ ) {
							if(curBrandNo.equals(sendData.get(k)[13])) {
								sbXML.append(" <E1BPOBDLVITEMCTRLCON SEGMENT=\"1\">")
								.append(" <DELIV_NUMB>").append(sendData.get(k)[26]).append("</DELIV_NUMB> ")
								.append(" <DELIV_ITEM>").append(sendData.get(k)[27]).append("</DELIV_ITEM> ")
								.append(" <CHG_DELQTY>").append(sendData.get(k)[28]).append("</CHG_DELQTY> ")
								.append(" </E1BPOBDLVITEMCTRLCON> ");
								if((k+1) >= sendData.size()) {
									sbXML.append(" <E1BPEXTC SEGMENT=\"1\"><FIELD1>")
									.append(sendData.get(k)[12])
									.append("</FIELD1></E1BPEXTC> ")
									.append(" </E1SHP_OBDLV_CONFIRM_DECENTR> ")
									.append(" </IDOC> ")
									.append(" </SHP_OBDLV_CONFIRM_DECENTRAL02> ");						
									doc = xmlMgr.createXmlDoc(sbXML.toString());
									xmlMgr.write(doc, prop.getProperty("send.upload.sourcePath")+"I_SHP_OB_"+sendData.get(k)[12]+".xml");
									logger.info(sbXML.toString());
									sbXML.delete(0, sbXML.length());
									
									linker.updateSucc(sendData.get(k)[12], "D10");
								}
								j=k+1;
							} else {
								break;
							}
						}
					} else {
						logger.error("else Error!");
					}
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
			
		} catch(SQLException e) {
			loggerErr.error(e.getMessage());
			loggerErr.error(e.toString());
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
