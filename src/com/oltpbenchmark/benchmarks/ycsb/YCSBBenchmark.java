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
        for (int i = 1; i <= 11; i++)
            rec.put(i, (String) a.get(i - 1));   
    }
    
    public static String YCSBRecToJson(Map<Integer, String> rec) {
        JSONArray a = new JSONArray();
        for (int i = 1; i <= 11; i++)
            a.add((String) rec.get(i));
        return a.toJSONString(JSONStyle.MAX_COMPRESS);
    }
    
    public YCSBBenchmark(WorkloadConfiguration workConf) {
        super("ycsb", workConf, true);
        XMLConfiguration xml = workConf.getXmlConfig();
        if (xml != null) {
            readUpdateAccessSkew = xml.getDouble("readUpdateAccessSkew");
            readUpdateDataSkew = xml.getDouble("readUpdateDataSkew");
        } else {
            readUpdateAccessSkew = 80.0;
            readUpdateDataSkew = 20.0;
        }
    }
    
    // percentages
    private final double readUpdateAccessSkew;
    private final double readUpdateDataSkew;

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
            //
            for (int i = 0; i < workConf.getTerminals(); ++i) {
//                Connection conn = this.makeConnection();
//                conn.setAutoCommit(false);
                workers.add(new YCSBWorker(i, this, init_record_count + 1, 
                      readUpdateAccessSkew, readUpdateDataSkew));
            } // FOR
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
