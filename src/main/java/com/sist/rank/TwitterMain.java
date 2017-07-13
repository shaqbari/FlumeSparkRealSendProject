package com.sist.rank;

import java.util.*;

import twitter4j.FilterQuery;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

public class TwitterMain {
	public static void main(String[] args) {
		try {
			List<String> list=RankData.naverRank();
			//List<String> list=RankData.daumRank();
			//String[] data=(String[]) list.toArray();
			String[] data=new String[list.size()];
			list.toArray(data);
			int i=0;
			for (String s : data) {
				System.out.println(s);
				
			}
			TwitterStream ts=new TwitterStreamFactory().getInstance();
			TwitterListener tList=new TwitterListener();
			ts.addListener(tList);
			
			FilterQuery fq=new FilterQuery();
			fq.track(data);
			ts.filter(fq);
			
		} catch (Exception e) {
			System.out.println("TwitterMain에서"+e.getMessage());
			e.printStackTrace();
		}
	}
}
