package com.rybak.log_analyzer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Rybak Nikita
 * 
 * Format for record in log file - Username,Time,Custom Message
 * Format for Time - "dd.MM.yyyy 'at' H:mm:ss" (e.g. 23.05.2015 at 22:39:28)
 * The maximum number of threads using to process files - 10, default - 1
 * 
 */

public class LogAnalyzer {
	
	private List<File> logs;
	private Queue<String> outputRecords;
	
	private String username;
	private String timePeriod;
	private String customMessage;
	private String groupingUsername;
	private String[] timeUnit;
	private int threadsCount;
	private String outputDirectory;
	private String directoryPath;
	
	public LogAnalyzer(String directoryPath, String filterUsername, String timePeriod, 
					   String customMessage, String groupingUsername, String timeUnit, 
					   int threadsCount, String outputDirectory){
		
		logs = Arrays.asList(new File(directoryPath).listFiles());
		outputRecords = new LinkedBlockingQueue<>();
		if (timeUnit != null) {
			this.timeUnit = timeUnit.split(" ");
		}
		this.username = filterUsername;
		this.timePeriod = timePeriod;
		this.customMessage = customMessage;
		this.groupingUsername = groupingUsername;
		this.threadsCount = threadsCount;
		this.outputDirectory = outputDirectory;
		this.directoryPath = directoryPath;
	    
	}
	
	public void analyze() throws IOException, ParseException, InterruptedException{
		if (threadsCount > 1) {
			ExecutorService executor;
			if (threadsCount > 10) {
				executor = Executors.newFixedThreadPool(10);
			} else {
				executor = Executors.newFixedThreadPool(threadsCount);
			}
			
			for (int i = 0; i < logs.size(); i++) {
				File log = logs.get(i);
				if (log.isFile()) {
					executor.execute(new Filter(log));
				}
			}
			executor.shutdown();
			while (!executor.isTerminated()) {
				Thread.sleep(500);
			}
		} else {
			filter();
		}
		if (groupingUsername == null && timeUnit == null) {
			throw new IllegalArgumentException();
		} else if (groupingUsername != null && timeUnit != null) {
			generateOutputWithAllStatistics();
		} else if (groupingUsername != null) {
			generateOutputWithUsernameStatistics();
		} else if (timeUnit != null) {
			generateOutputWithTimeUnitStatistics();
		} else {
			generateOutput();
		}
	}
	
	private boolean containsTimeUnit(Date date, String[] timeUnit) {
		
		if (timeUnit[1].equals("second")) {
			int second = Integer.valueOf(timeUnit[0]);
			if (date.getSeconds() == second) {
				return true;
			}
		} else if (timeUnit[1].equals("minute")) {
			int minute = Integer.valueOf(timeUnit[0]);
			if (date.getMinutes()  == minute) {
				return true;
			}
		} else if (timeUnit[1].equals("hour")) {
			int hour = Integer.valueOf(timeUnit[0]);
			if (date.getHours()  == hour) {
				return true;
			}
		}  else if (timeUnit[1].equals("day")) {
			int day = Integer.valueOf(timeUnit[0]);
			if (date.getDate()  == day) {
				return true;
			}
		}  else if (timeUnit[1].equals("month")) {
			int month = Integer.valueOf(timeUnit[0]);
			if (date.getMonth()+1  == month) {
				return true;
			}
		} else if (timeUnit[1].equals("year")) {
			int year = Integer.valueOf(timeUnit[0]);
			if (date.getYear()+1900  == year) {
				return true;
			}
		}
		
		return false;
	}
	
	private void filter() throws IOException, ParseException{
		
		if (username == null && timePeriod == null && customMessage == null) {
			throw new IllegalArgumentException();
		}
		List<String> inputRecords = getRecordsFromLogs();
		if (username != null) {
			List<String> filteredRecords = new ArrayList<>();

	        for (String record : inputRecords) {
	        	String recordUsername = getUsernameFromRecord(record);
	        	if (recordUsername.equals(username)) {
	        		filteredRecords.add(record);
	        	}
	        }
	        inputRecords = filteredRecords;
		}
		if (timePeriod != null) {
			List<String> filteredRecords = new ArrayList<>();
	        String[] period = timePeriod.split("-");
	        Date startPeriod = convertStringToDate(period[0]);
	        Date endPeriod = convertStringToDate(period[1]);

	        for (String record : inputRecords) {
	        	Date time = getTimeFromRecord(record);
	        	if (time.after(startPeriod) && time.before(endPeriod)) {
	        		filteredRecords.add(record);
	        	}
	        }
	        inputRecords = filteredRecords;
		}
		if (customMessage != null) {
			List<String> filteredRecords = new ArrayList<>();

	        for (String record : inputRecords) {
	        	String recordCustomMessage = getMessageFromRecord(record);
	        	if (recordCustomMessage.contains(customMessage)) {
	        		filteredRecords.add(record);
	        	}
	        }
	        inputRecords = filteredRecords;
		}
		outputRecords.addAll(inputRecords);
	}
	
	private Date convertStringToDate(String dateString) throws ParseException {
		
		SimpleDateFormat format = new SimpleDateFormat("dd.MM.yyyy 'at' H:mm:ss");
        Date date = format.parse(dateString);
		
		return date;
	}
	
	private List<String> getRecordsFromLogs() throws IOException {
		
        String line = "";
        List<String> records = new ArrayList<>();

        for (File log : logs) {
        	if (log.isFile()) {
        		try (BufferedReader br = new BufferedReader(new FileReader(log))) {
    	            while ((line = br.readLine()) != null) {
    	            	records.add(line);
    	            }
    	        } 
        	}
        }
		
		return records;
	}

	private List<String> getRecordsFromLog(File log) throws IOException {
		
        String line = "";
        List<String> records = new ArrayList<>();

        	if (log.isFile()) {
        		try (BufferedReader br = new BufferedReader(new FileReader(log))) {
    	            while ((line = br.readLine()) != null) {
    	            	records.add(line);
    	            }
    	        } 
        	}
		
		return records;
	}
	
	private String getUsernameFromRecord(String record) {
		String[] splittedRecord = record.split(","); 
		return splittedRecord[0];
	}
	
	private Date getTimeFromRecord(String record) throws ParseException {
		String[] splittedRecord = record.split(","); 
		SimpleDateFormat format = new SimpleDateFormat("dd.MM.yyyy 'at' H:mm:ss");
        Date date = format.parse(splittedRecord[1]);
		return date;
	}
	
	private String getMessageFromRecord(String record) {
		String[] splittedRecord = record.split(","); 
		return splittedRecord[2];
	}
	
	private void generateOutput() throws IOException {
		File file = new File(outputDirectory);
		if (!file.isAbsolute()) {
			file = new File(directoryPath + "/" + outputDirectory);
		}
		
		if (!file.exists()) {
			file.createNewFile();
		}
		try (FileWriter fw = new FileWriter(file.getAbsoluteFile());
				BufferedWriter bw = new BufferedWriter(fw)) {
			
			while (!outputRecords.isEmpty()) {
				bw.write(outputRecords.poll() + "\n");
			}
		}
	}
	
	private void generateOutputWithAllStatistics() throws IOException, ParseException {
		
		File file = new File(outputDirectory);
		if (!file.isAbsolute()) {
			file = new File(directoryPath + "/" + outputDirectory);
		}
		int countOfRecordsWithUsername = 0;
		int countOfRecordsWithTimeUnit = 0;
		
		if (!file.exists()) {
			file.createNewFile();
		}
		try (FileWriter fw = new FileWriter(file.getAbsoluteFile());
				BufferedWriter bw = new BufferedWriter(fw)) {
			
			String currentRecord = "";
			while (!outputRecords.isEmpty()) {
				currentRecord = outputRecords.poll();
				
				if (currentRecord.contains(groupingUsername)) {
					countOfRecordsWithUsername++;
			    }
				Date time = getTimeFromRecord(currentRecord);
				if (containsTimeUnit(time, timeUnit)) {
					countOfRecordsWithTimeUnit++;
			    }
				
				bw.write(currentRecord + "\n");
			}
			
			bw.write("\n" + "The number of records with the username " + groupingUsername + " = " 
					+ countOfRecordsWithUsername + "\n");
			bw.write("\n" + "The number of records with the " + timeUnit[0] + " " + timeUnit[1]
					+ " = " + countOfRecordsWithTimeUnit + "\n");
		}
		
	}
	
	private void generateOutputWithUsernameStatistics() throws IOException {
		File file = new File(outputDirectory);
		if (!file.isAbsolute()) {
			file = new File(directoryPath + "/" + outputDirectory);
		}
		int countOfRecordsWithUsername = 0;
		
		if (!file.exists()) {
			file.createNewFile();
		}
		try (FileWriter fw = new FileWriter(file.getAbsoluteFile());
				BufferedWriter bw = new BufferedWriter(fw)) {
			
			String currentRecord = "";
			while (!outputRecords.isEmpty()) {
				currentRecord = outputRecords.poll();
				if (currentRecord.contains(groupingUsername)) {
					countOfRecordsWithUsername++;
			    }
				bw.write(currentRecord + "\n");
			}
			bw.write("\n" + "The number of records with the username " + groupingUsername + " = " 
					+ countOfRecordsWithUsername + "\n");
		}
	}
	
	private void generateOutputWithTimeUnitStatistics() throws IOException, ParseException {
		File file = new File(outputDirectory);
		if (!file.isAbsolute()) {
			file = new File(directoryPath + "/" + outputDirectory);
		}
		int countOfRecordsWithTimeUnit = 0;
		
		if (!file.exists()) {
			file.createNewFile();
		}
		try (FileWriter fw = new FileWriter(file.getAbsoluteFile());
				BufferedWriter bw = new BufferedWriter(fw)) {
			
			String currentRecord = "";
			while (!outputRecords.isEmpty()) {
				currentRecord = outputRecords.poll();
				Date time = getTimeFromRecord(currentRecord);
				if (containsTimeUnit(time, timeUnit)) {
					countOfRecordsWithTimeUnit++;
			    }
				bw.write(currentRecord + "\n");
			}
			bw.write("\n" + "The number of records with the " + timeUnit[0] + " " + timeUnit[1]
					+ " = " + countOfRecordsWithTimeUnit + "\n");
		}
	}
	
	private class Filter implements Runnable{
			
		private File log;
			
		public Filter(File log) {
			this.log = log;
		}
			
			@Override
		public void run(){
			if (username == null && timePeriod == null && customMessage == null) {
				throw new IllegalArgumentException();
			}
			List<String> inputRecords = null;
			try {
				inputRecords = getRecordsFromLog(log);
			} catch (IOException e) {
				e.printStackTrace();
			}
			if (username != null) {
				List<String> filteredRecords = new ArrayList<>();
				
			    for (String record : inputRecords) {
			        String recordUsername = getUsernameFromRecord(record);
			        if (recordUsername.equals(username)) {
			        	filteredRecords.add(record);
			        }
			    }
			    inputRecords = filteredRecords;
			}
			if (timePeriod != null) {
				List<String> filteredRecords = new ArrayList<>();
				String[] period = timePeriod.split("-");
				Date startPeriod = null;
				Date endPeriod = null;
				try {
					startPeriod = convertStringToDate(period[0]);
					endPeriod = convertStringToDate(period[1]);
				} catch (ParseException e) {
					e.printStackTrace();
				}

				for (String record : inputRecords) {
					Date time = null;
					try {
						time = getTimeFromRecord(record);
					} catch (ParseException e) {
						e.printStackTrace();
					}
					if (time.after(startPeriod) && time.before(endPeriod)) {
						filteredRecords.add(record);
					}
				}
				inputRecords = filteredRecords;
			}
			if (customMessage != null) {
				List<String> filteredRecords = new ArrayList<>();

				for (String record : inputRecords) {
					String recordCustomMessage = getMessageFromRecord(record);
					if (recordCustomMessage.contains(customMessage)) {
						filteredRecords.add(record);
					}
				}
				inputRecords = filteredRecords;
			}
			outputRecords.addAll(inputRecords);
		}
	}
}
