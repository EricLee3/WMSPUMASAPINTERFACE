package com.gics.edi.puma.main;

import java.util.Calendar;
import java.util.Timer;

import org.apache.log4j.Logger;

import com.gics.edi.puma.task.TaskPumaSendInbound;
import com.gics.edi.puma.task.TaskPumaSendOutbound;

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
			
			// else outbound
			} else {
				Calendar cal = Calendar.getInstance();
				cal.set(Calendar.HOUR_OF_DAY, 22);
				cal.set(Calendar.MINUTE, 0);
				cal.set(Calendar.SECOND, 0);
				
				t1.schedule(new TaskPumaSendOutbound(), cal.getTime(), 86400000);
			}
			logger.info("PumaSendLinker End!!");
		} catch (Exception e) {
			loggerErr.error(e.getMessage());
			loggerErr.error(e.toString());
		}
	}
}
