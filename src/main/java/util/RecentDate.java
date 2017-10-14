package util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * Created by hzliulongfei on 2017/10/12/0012.
 */
public class RecentDate {
    public static Calendar calendar = Calendar.getInstance();
    public static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    public static List<String> getDateNDaysBefore(String startDate, int nDays)
            throws ParseException {
        List<String> dateStr = new ArrayList<String>();
        Date date = dateFormat.parse(startDate);
        for(int i=0; i<nDays; i++){
            calendar.setTime(date);
            calendar.add(Calendar.DATE, -i);
            dateStr.add(dateFormat.format(calendar.getTime()));
        }
        return dateStr;
    }

    public static List<String> getDateNDaysAfter(String startDate, int nDays)
            throws ParseException{

        List<String> dateStr = new ArrayList<String>();
        Date date = dateFormat.parse(startDate);
        for(int i=0; i<nDays; i++){
            calendar.setTime(date);
            calendar.add(Calendar.DATE, i);
            dateStr.add(dateFormat.format(calendar.getTime()));
        }

        return dateStr;
    }

    public static void main(String[] args) throws ParseException{
        String startDate = "2017-07-20";
        List<String> days = getDateNDaysBefore(startDate, 2);
        for(String each : days){
            System.out.println(each);
        }
    }
}
