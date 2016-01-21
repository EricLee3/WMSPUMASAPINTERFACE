package com.gics.edi.puma.main;

import java.text.SimpleDateFormat;






import java.util.Calendar;
import java.util.Date;
import java.util.Timer;

import org.apache.log4j.Logger;

import com.gics.edi.puma.task.TaskPumaSendInbound;
import com.gics.edi.puma.task.TaskPumaSendOutbound;
import com.gics.edi.puma.task.TaskPumaSendInboundTmp;





public class PumaSendLinker {

	/**
	 * @param args
	 */
	private static Logger loggerErr = Logger.getLogger("process.puma.err");
	private static Logger logger = Logger.getLogger("process.puma");
	
	public static void main(String[] args) {
		logger.info("PumaSendLinker Start!!");
		Timer t1 = new Timer(false);
		int repTime = 0;
		
		try {
			if("sendInbound".equals(args[0])) {
				repTime = (1000*60*180); // 3시간
				t1.schedule(new TaskPumaSendInbound(), 0, repTime);
			} else if("sendInbound1".equals(args[0])) {
				//t1.schedule(new TaskPumaSendInboundTmp(), 1000);
			}  else {				
				String startDtOutbound = "2016-01-21 22:00:00";
				SimpleDateFormat transFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				Date startDt = transFormat.parse(startDtOutbound);
				
				t1.schedule(new TaskPumaSendOutbound(), startDt, 86400000);
				// PumaSendLinker_EXEC.jar  sendOutbound
				//t1.schedule(new TaskPumaSendOutbound(), 1000);
			}
			logger.info("PumaSendLinker End!!");
		} catch (Exception e) {
			loggerErr.error(e.getMessage());
			loggerErr.error(e.toString());
		}
	}
}
