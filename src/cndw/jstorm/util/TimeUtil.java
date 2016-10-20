package cndw.jstorm.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class TimeUtil {
	/**
	 * 获取分钟数
	 * @param ts
	 * @return
	 */
	public static int getMinute(int ts){
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(new Date(ts));
		return calendar.get(Calendar.MINUTE);
	}
	
	/**
	 * 将微秒以指定格式输出为本地时间字符串
	 * @param millis
	 * @param format  yyyy-MM-dd HH:mm:ss
	 * @return
	 */
	public static String toTime(int secand, String format) {
		Date date = new Date((long)secand*1000);
		SimpleDateFormat formatter = new SimpleDateFormat(format);
		return formatter.format(date);
	}
	
	public static String yestoday(long secand,String format){
		Date date = new Date((long)secand*1000 - 24*60*60*1000);
		SimpleDateFormat formatter = new SimpleDateFormat(format);
		return formatter.format(date);
		
	}
	
	/**
	 * 五分钟指标
	 * @param mm
	 * @return
	 */
	public static String to5minute(String mm){
		String m1 = "";
		int m = Integer.valueOf(mm);
		if(m<5){
			m1 = "05";
		}else if(m<10){
			m1="10";
		}else if(m<15){
			m1="15";
		}else if(m<20){
			m1="20";
		}else if(m<25){
			m1="25";
		}else if(m<30){
			m1="30";
		}else if(m<35){
			m1="35";
		}else if(m<40){
			m1="40";
		}else if(m<45){
			m1="45";
		}else if(m<50){
			m1="50";
		}else if(m<55){
			m1="55";
		}else if(m>=55){
			m1="60";
		}
		return m1;
	}
	
	/**
	 * 10分钟指标
	 * @param mm
	 * @return
	 */
	public static String to10minute(String mm){
		String m1 = "";
		int m = Integer.valueOf(mm);
		if(m<10){
			m1 = "10";
		}else if(m<20){
			m1="20";
		}else if(m<30){
			m1="30";
		}else if(m<40){
			m1="40";
		}else if(m<50){
			m1="50";
		}else if(m>=50){
			m1="60";
		}
		return m1;
	}
	
	/**
	 * 30分钟指标
	 * @param mm
	 * @return
	 */
	public static String to30minute(String mm){
		String m1 = "";
		int m = Integer.valueOf(mm);
		if(m<30){
			m1 = "30";
		}else if(m<60){
			m1="60";
		}
		return m1;
	}
	
	/**
	 * 1小时指标
	 * @param mm
	 * @return
	 */
	public static String to1Hour(String hh){
		String h1 = "";
		int h = Integer.valueOf(hh);
		if(h<1){h1 = "01";}
		else if(h<2){h1="02";}
		else if(h<3){h1="03";}
		else if(h<4){h1="04";}
		else if(h<5){h1="05";}
		else if(h<6){h1="06";}
		else if(h<7){h1="07";}
		else if(h<8){h1="08";}
		else if(h<9){h1="09";}
		else if(h<10){h1="10";}
		else if(h<11){h1="11";}
		else if(h<12){h1="12";}
		else if(h<13){h1="13";}
		else if(h<14){h1="14";}
		else if(h<15){h1="15";}
		else if(h<16){h1="16";}
		else if(h<17){h1="17";}
		else if(h<18){h1="18";}
		else if(h<19){h1="19";}
		else if(h<20){h1="20";}
		else if(h<21){h1="21";}
		else if(h<22){h1="22";}
		else if(h<23){h1="23";}
		else if(h<24){h1="24";}
		return h1;
	}
	
	public static void main(String[] args) {
		System.out.println(yestoday(System.currentTimeMillis()/1000, "yyyy-MM-dd"));
	}
}
