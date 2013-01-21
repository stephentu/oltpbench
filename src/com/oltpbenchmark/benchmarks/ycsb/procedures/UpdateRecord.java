package com.oltpbenchmark.benchmarks.ycsb.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;

import net.spy.memcached.MemcachedClient;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.ycsb.YCSBBenchmark;
import com.oltpbenchmark.benchmarks.ycsb.YCSBConstants;
import com.oltpbenchmark.memcached.MemcachedClientIface;

import com.google.code.hs4j.FindOperator;
import com.google.code.hs4j.IndexSession;
import com.google.code.hs4j.ModifyStatement;

public class UpdateRecord extends Procedure {

    private static final Logger LOG = Logger.getLogger(ReadRecord.class);
    
    public final SQLStmt updateAllStmt = new SQLStmt(
        "UPDATE USERTABLE SET FIELD1=?,FIELD2=?,FIELD3=?,FIELD4=?,FIELD5=?," +
        "FIELD6=?,FIELD7=?,FIELD8=?,FIELD9=?,FIELD10=? WHERE YCSB_KEY=?"
    );
    
    public void run(Connection conn, MemcachedClientIface mcclient, IndexSession userTable,
                    boolean useHS, int keyname, Map<Integer,String> vals) throws SQLException {
        if (useHS) {
            try {
                ModifyStatement stmt = userTable.createStatement();
                stmt.setInt(1, keyname);
                for(Entry<Integer, String> s:vals.entrySet())
                {
                    stmt.setString(s.getKey() + 1, s.getValue());
                }
                stmt.update(new String[] { "" + keyname }, FindOperator.EQ);
            } catch (Exception e) {
                throw new SQLException(e);
            }
        } else {
            PreparedStatement stmt = this.getPreparedStatement(conn, updateAllStmt);
            assert(vals.size()==10);       
            stmt.setInt(11,keyname); 
            for(Entry<Integer, String> s:vals.entrySet())
            {
                stmt.setString(s.getKey(), s.getValue());
            }
            stmt.executeUpdate();
        }

        if (mcclient != null) {
            try { 
                // put the right record
                mcclient.set(YCSBBenchmark.MCKey(keyname), YCSBConstants.MC_KEY_TIMEOUT, YCSBBenchmark.YCSBRecToJson(vals));
            } catch (IllegalStateException e) {
                // queue is too full
                LOG.warn("MC queue is full", e);
            }
        }
    }
}
