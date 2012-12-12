package com.oltpbenchmark.benchmarks.ycsb.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;

import net.spy.memcached.MemcachedClient;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.ycsb.YCSBBenchmark;

public class UpdateRecord extends Procedure {
    
    public final SQLStmt updateAllStmt = new SQLStmt(
        "UPDATE USERTABLE SET FIELD1=?,FIELD2=?,FIELD3=?,FIELD4=?,FIELD5=?," +
        "FIELD6=?,FIELD7=?,FIELD8=?,FIELD9=?,FIELD10=? WHERE YCSB_KEY=?"
    );
    
    public void run(Connection conn, MemcachedClient mcclient, int keyname, Map<Integer,String> vals) throws SQLException {
        if (mcclient != null) {
            // do mc invalidation
            mcclient.delete(YCSBBenchmark.MCKey(keyname));
        }
        
    	PreparedStatement stmt = this.getPreparedStatement(conn, updateAllStmt);
		assert(vals.size()==10);       
		stmt.setInt(11,keyname); 
        for(Entry<Integer, String> s:vals.entrySet())
        {
        	stmt.setString(s.getKey(), s.getValue());
        }
        stmt.executeUpdate();
    }
}
