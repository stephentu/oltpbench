package com.oltpbenchmark.benchmarks.ycsb;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONStyle;
import net.minidev.json.JSONValue;

import org.apache.commons.configuration.XMLConfiguration;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.ycsb.procedures.InsertRecord;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;

public class YCSBBenchmark extends BenchmarkModule {

    public static String MCKey(int ycsb_key) {
        return "ycsb:ycsb_key:" + ycsb_key;
    }
    
    public static Map<Integer, String> YCSBRecFromJson(String s) {
        Map<Integer, String> ret = new HashMap<Integer, String>();
        YCSBRecFromJson(s, ret);
        return ret;
    }
    
    public static void YCSBRecFromJson(String s, Map<Integer, String> rec) {
        JSONArray a = (JSONArray) JSONValue.parse(s);
        if (a == null)
            throw new RuntimeException("bad json: " + s);
        for (int i = 1; i < 11; i++) {
            Object o = a.get(i - 1);
            rec.put(i, o.toString());
        }
    }
    
    public static String YCSBRecToJson(Map<Integer, String> rec) {
        JSONArray a = new JSONArray();
        for (int i = 1; i < 11; i++)
            a.add((String) rec.get(i));
        return a.toJSONString();
    }
    
    public YCSBBenchmark(WorkloadConfiguration workConf) {
        super("ycsb", workConf, true);
        XMLConfiguration xml = workConf.getXmlConfig();
        if (xml != null) {
            readUpdateHotAccessSkew = xml.getDouble("hotAccessSkew");
            readUpdateHotDataSkew = xml.getDouble("hotDataSkew");
            readUpdateWarmAccessSkew = xml.getDouble("warmAccessSkew");
            readUpdateWarmDataSkew = xml.getDouble("warmDataSkew");
            memcachedWarmup = xml.getInt("memcachedMemPercent");
        } else {
            readUpdateHotAccessSkew = 80.0;
            readUpdateHotDataSkew = 20.0;
            readUpdateWarmAccessSkew = 10.0;
            readUpdateWarmDataSkew = 10.0;
            memcachedWarmup = 100;
        }

        if (memcachedWarmup < 0 || memcachedWarmup > 100)
          throw new IllegalArgumentException("bad warmup percentage");
    }
    
    // percentages
    private final double readUpdateHotAccessSkew;
    private final double readUpdateHotDataSkew;
    private final double readUpdateWarmAccessSkew;
    private final double readUpdateWarmDataSkew;
    
    private final int memcachedWarmup;

    @Override
    protected List<Worker> makeWorkersImpl(boolean verbose) throws IOException {
        ArrayList<Worker> workers = new ArrayList<Worker>();
        try {
            Connection metaConn = this.makeConnection();

            // LOADING FROM THE DATABASE IMPORTANT INFORMATION
            // LIST OF USERS

            Table t = this.catalog.getTable("USERTABLE");
            assert (t != null) : "Invalid table name '" + t + "' " + this.catalog.getTables();
            String userCount = SQLUtil.getMaxColSQL(t, "ycsb_key");
            Statement stmt = metaConn.createStatement();
            ResultSet res = stmt.executeQuery(userCount);
            int init_record_count = 0;
            while (res.next()) {
                init_record_count = res.getInt(1);
            }
            assert init_record_count > 0;
            res.close();

            for (int i = 0; i < workConf.getTerminals(); ++i) {
//                Connection conn = this.makeConnection();
//                conn.setAutoCommit(false);
                workers.add(new YCSBWorker(i, this, init_record_count + 1, 
                      readUpdateHotAccessSkew, 
                      readUpdateHotDataSkew,
                      readUpdateWarmAccessSkew, 
                      readUpdateWarmDataSkew));
            } // FOR

            if (memcachedWarmup > 0) {
                YCSBWorker w = (YCSBWorker) workers.get(0);
                Map<Integer, String> rec = w.readRecord(0); 
                for (int i = 0; i < (int)(((double)init_record_count) * ((double)memcachedWarmup)/100.0); i++) {
                    w.putInMC(i, rec);
                    //if (((i+1) % 10000) == 0)
                    //  System.out.println("i elems: " + (i+1));
                }
            }

            metaConn.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return workers;
    }

    @Override
    protected Loader makeLoaderImpl(Connection conn) throws SQLException {
        return new YCSBLoader(this, conn);
    }

    @Override
    protected Package getProcedurePackageImpl() {
        // TODO Auto-generated method stub
        return InsertRecord.class.getPackage();
    }

}
