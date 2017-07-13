package com.sist.rank;


import org.apache.log4j.Logger;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.Twitter;
import java.io.*;

public class TwitterListener implements StatusListener{
	
	Logger logger=Logger.getLogger(TwitterListener.class);//반복을 없애준다.
	
	
	@Override
	public void onException(Exception e) {
		System.out.println(e.getMessage());
	}

	@Override
	public void onDeletionNotice(StatusDeletionNotice arg0) {
		
	}

	@Override
	public void onScrubGeo(long arg0, long arg1) {
		
	}

	@Override
	public void onStallWarning(StallWarning arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onStatus(Status status) {
		try{
			String data=status.getText();
			System.out.println(data);
			try {
				FileWriter fw=new FileWriter("./input/naver.txt", true);//true주면 append모드 //input폴더를 만들어줘야 한다.
				//FileWriter fw=new FileWriter("./input/daum.txt", true);//true주면 append모드 //input폴더를 만들어줘야 한다.
				fw.write(data);
				fw.close();
				
			} catch (Exception e) {
				
			}
			
		} catch (Exception e) {
			System.out.println(e.getMessage()); 
		}
		
	}

	@Override
	public void onTrackLimitationNotice(int arg0) {
		// TODO Auto-generated method stub
		
	}


}
