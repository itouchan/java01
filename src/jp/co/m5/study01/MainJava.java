package jp.co.m5.study01;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;

public class MainJava {

	public static void main(String[] args) {
		System.out.println("- START -");
		long start = System.currentTimeMillis();

		try {
			// CSVファイル読込み
			ArrayList<String> regList = readCsv();
			// DB登録
			writeDb(regList);
		} catch (IOException e) {
			e.printStackTrace();
		}

		long end = System.currentTimeMillis();
		System.out.println("実行時間:" + (end - start) + "ミリ秒");
		System.out.println("- END -");
	}

	public static ArrayList<String> readCsv() throws IOException {
		ArrayList<String> result = new ArrayList<String>();
		String csvPath = ".\\resource\\insert.csv";
		FileReader in = null;
		BufferedReader br = null;
		String line = null;
		int idx = 0;
		try {
			in = new FileReader(csvPath);
			br = new BufferedReader(in);
			while ((line = br.readLine()) != null ) {
				idx++;
				if (idx == 1) {
					continue;
				}
				result.add(line);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
			if (in != null) {
				in.close();
			}
			if (br != null) {
				br.close();
			}
		}
		return result;
	}

	public static void writeDb(ArrayList<String> list) {
		Connection con = getCon();
		int regCount = 0;
		if (con == null) {
			System.out.println("Error:DB接続エラー");
			return;
		}

		if (list == null || list.size() == 0) {
			return;
		}

		try {
			for(String data : list){
				String datas[] = data.split(",");
				String sql = "INSERT INTO emp (name, birth, group_id, created_by) VALUES (?, ?, ?, ?)";
				String name = datas[0].replaceAll("\"", "");
				int grpId = Integer.parseInt(datas[2].replaceAll("\"", ""));
				int creater = Integer.parseInt(datas[3].replaceAll("\"", ""));
				String birth = datas[1].replaceAll("\"", "");
				int year = Integer.parseInt(birth.substring(0, 4));
				int month = Integer.parseInt(birth.substring(5, 7)) - 1;
				int day = Integer.parseInt(birth.substring(8, 10));
				Calendar cal = Calendar.getInstance();
				cal.set(Calendar.YEAR, year);
				cal.set(Calendar.MONTH, month);
				cal.set(Calendar.DATE, day);
				Date birthDt = new java.sql.Date(cal.getTimeInMillis());
				PreparedStatement pstmt = con.prepareStatement(sql);
				pstmt.setString(1, name);
				pstmt.setDate(2, birthDt);
				pstmt.setInt(3, grpId);
				pstmt.setInt(4, creater);
				int b = pstmt.executeUpdate();
				if (b > 0) {
					regCount++;
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("登録件数:" + regCount + "件");
	}

	public static Connection getCon() {
		Connection con = null;
		String className = "com.mysql.jdbc.Driver";
		String host = "HOST";
		String port = "3306";
		String schema = "DB";
		String user = "USER";
		String pass = "PASS";
		String url = "jdbc:mysql://" + host + ":" + port + "/" + schema + "?autoReconnect=true&useSSL=false";

		try {
			Class.forName(className).newInstance();
		    con = DriverManager.getConnection(url, user, pass);
		} catch (InstantiationException | IllegalAccessException
				| ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
		return con;
	}

}
