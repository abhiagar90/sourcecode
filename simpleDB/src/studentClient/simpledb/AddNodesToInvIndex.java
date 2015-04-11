package studentClient.simpledb;

import java.sql.*;

import simpledb.remote.SimpleDriver;

public class AddNodesToInvIndex {
	public static void main(String[] args) {
		Connection conn = null;
		try {
			Driver d = new SimpleDriver();
			conn = d.connect("jdbc:simpledb://localhost", null);
			Statement stmt = conn.createStatement();

			String s = "insert into nono(name )values ";
			
			int leftnum = 1;
			int rightnum = 1;
			for(leftnum=2;leftnum<=99;leftnum++)
			{
				for(rightnum=2;rightnum<=99;rightnum++)
				{
					System.out.println("inserting: "+"(" + leftnum +" " + rightnum +");");
					stmt.executeUpdate(s + "('" + leftnum +" " + rightnum +"');");
				}
			}
			System.out.println("STUDENT records inserted.");

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
