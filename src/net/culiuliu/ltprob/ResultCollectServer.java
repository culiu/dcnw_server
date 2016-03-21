package net.culiuliu.ltprob;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

import net.culiuliu.hbase.Prob_server_db_client;

public class ResultCollectServer implements Runnable
{
  Socket csocket;
  ResultCollectServer(Socket csocket)
  {
    this.csocket = csocket;
  }
  
  public static void main(String args[])
  throws Exception
  {
    ServerSocket ssocket = new ServerSocket(1234);
    System.out.println("Listening");
    while (true) {
      Socket sock = ssocket.accept();
      System.out.println("Connected");
      new Thread(new ResultCollectServer(sock)).start();
    }
  }
  
  public void run()
  {
	BufferedReader in = null;
    try {
	  in = new BufferedReader(new InputStreamReader(csocket.getInputStream()));
	} catch (IOException e) {
		// TODO Auto-generated catch block
	  e.printStackTrace();
	}
    String result = "";
    String line = "";
    try {
		while((line = in.readLine()) != null) {
		  result += line;
		}
		csocket.close();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    Prob_server_db_client.insert_stats(result);
  }
}
