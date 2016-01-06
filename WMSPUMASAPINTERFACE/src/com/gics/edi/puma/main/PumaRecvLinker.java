package com.gics.edi.puma.main;

import java.util.Timer;
import org.apache.log4j.Logger;
import com.gics.edi.puma.task.TaskPumaRecv;

public class PumaRecvLinker {
	private static Logger loggerErr = Logger.getLogger("process.puma.err");
	private static Logger logger = Logger.getLogger("process.puma");
	
	public static void main(String[] args) {
		logger.info("PumaRecvLinker Start!!");
		Timer t1 = new Timer(false);
		int repTime = (1000*60*60); // 1시간
		try {
			t1.schedule(new TaskPumaRecv(), 0, repTime);
			logger.info("PumaRecvLinker End!!");
		} catch (Exception e) {
			loggerErr.error(e.getMessage());
			loggerErr.error(e.toString());
		}
	}
}
//	private static Integer[] getLastFile() {
//		PropManager propMgr = PropManager.getInstances();
//		Properties fileProp = propMgr.getProp("fileInfo","puma");
//				
//		int cnt = 1;
//		Integer[] lastNums = new Integer[8];
//		while(fileProp.getProperty("file.lastFile"+cnt)!=null) {
//			String f = fileProp.getProperty("file.lastFile"+cnt);
//			if(f.length()>=12) {
//				String subString = f.substring(f.lastIndexOf("_")+1, f.length()).replaceAll(".xml", "");
//				lastNums[cnt-1] = Integer.parseInt(subString);
//			} else {
//				lastNums[cnt-1] = Integer.parseInt(f);
//			}
//			cnt++;
//		}
//		return lastNums;
//	}

