package com.oltpbenchmark.benchmarks.ycsb.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import net.spy.memcached.MemcachedClient;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.ycsb.YCSBBenchmark;
import com.oltpbenchmark.benchmarks.ycsb.YCSBConstants;
import com.oltpbenchmark.memcached.MemcachedClientIface;

import com.google.code.hs4j.IndexSession;

public class ReadRecord extends Procedure{
    public final SQLStmt readStmt = new SQLStmt(
        "SELECT * FROM USERTABLE WHERE YCSB_KEY=?"
    );
    
    private static final Logger LOG = Logger.getLogger(ReadRecord.class);
    
	//FIXME: The value in ysqb is a byteiterator
    public void run(Connection conn, MemcachedClientIface mcclient, IndexSession userTable,
                    boolean useHS, int keyname, Map<Integer, String> results) throws SQLException {
        
        if (mcclient != null) {
            // check MC
            Object o = null;
            if ((o = mcclient.get(YCSBBenchmark.MCKey(keyname))) != null) {
                YCSBBenchmark.YCSBRecFromJson((String) o, results);
                return;
            }
        }
        
        if (useHS) {
            try {
                ResultSet r = userTable.find(new String[] { "" + keyname });
                while (r.next()) {
                    for(int i=2;i<=11;i++)
                        results.put(i - 1, r.getString(i));
                }
                r.close();
            } catch (Exception e) {
                throw new SQLException(e);
            }
        } else {
            PreparedStatement stmt = this.getPreparedStatement(conn, readStmt);
            stmt.setInt(1, keyname);          
            ResultSet r=stmt.executeQuery();
            while(r.next())
            {
                for(int i=1;i<11;i++)
                    results.put(i, r.getString(i));
            }
            r.close();
        }
        
        if (mcclient != null) {
            try { 
                mcclient.set(YCSBBenchmark.MCKey(keyname), YCSBConstants.MC_KEY_TIMEOUT, YCSBBenchmark.YCSBRecToJson(results));
            } catch (IllegalStateException e) {
                // queue is too full
                LOG.warn("MC queue is full", e);
            }
        }
    }

}
