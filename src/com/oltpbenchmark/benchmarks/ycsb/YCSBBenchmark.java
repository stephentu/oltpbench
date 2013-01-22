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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONValue;

import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;

import com.google.code.hs4j.IndexSession;
import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.ycsb.procedures.InsertRecord;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;

public class YCSBBenchmark extends BenchmarkModule {
    private static final Logger LOG = Logger.getLogger(YCSBBenchmark.class);

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
            memcachedWarmup = xml.getInt("memcachedWarmup");
        } else {
            readUpdateHotAccessSkew = 80.0;
            readUpdateHotDataSkew = 20.0;
            readUpdateWarmAccessSkew = 10.0;
            readUpdateWarmDataSkew = 10.0;
            memcachedWarmup = 100;
        }

        if (memcachedWarmup < 0 || memcachedWarmup > 100)
          throw new IllegalArgumentException("bad warmup percentage");

        useHS = xml.getBoolean("useHS");
    }
    
    // percentages
    private final double readUpdateHotAccessSkew;
    private final double readUpdateHotDataSkew;
    private final double readUpdateWarmAccessSkew;
    private final double readUpdateWarmDataSkew;
    
    private final int memcachedWarmup;
    private final boolean useHS;

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

            IndexSession readIdx = null, rwIdx = null;

            if (useHS) {
              try {
                readIdx = this.makeHSReadClient().openIndexSession(workConf.getDBName(), "USERTABLE", "PRIMARY", 
                    new String[] { "YCSB_KEY", "FIELD1", "FIELD2", "FIELD3", "FIELD4", "FIELD5",
                      "FIELD6", "FIELD7", "FIELD8", "FIELD9", "FIELD10" });
              } catch (Exception ex) {
                LOG.warn("fixme", ex);
              }

              try {
                rwIdx = this.makeHSReadWriteClient().openIndexSession(workConf.getDBName(), "USERTABLE", "PRIMARY", 
                    new String[] { "YCSB_KEY", "FIELD1", "FIELD2", "FIELD3", "FIELD4", "FIELD5",
                      "FIELD6", "FIELD7", "FIELD8", "FIELD9", "FIELD10" });
              } catch (Exception ex) {
                LOG.warn("fixme", ex);
              }
            }

            for (int i = 0; i < workConf.getTerminals(); ++i) {
//                Connection conn = this.makeConnection();
//                conn.setAutoCommit(false);
                workers.add(new YCSBWorker(i, this, init_record_count + 1, 
                      readUpdateHotAccessSkew, 
                      readUpdateHotDataSkew,
                      readUpdateWarmAccessSkew, 
                      readUpdateWarmDataSkew,
                      useHS, readIdx, rwIdx));
            } // FOR

            if (memcachedWarmup > 0) {
                int nkeys = (int)(((double)init_record_count) * ((double)memcachedWarmup)/100.0);
                int nkeyspertask = nkeys / workers.size();
                List<Runnable> tasks = new ArrayList<Runnable>();
                YCSBWorker w = (YCSBWorker) workers.get(0);
                final Map<Integer, String> rec = w.readRecord(0); 
                for (int i = 0; i < workers.size(); i++) {
                    final YCSBWorker thisW = (YCSBWorker) workers.get(i);
                    final int begin = i * nkeyspertask;
                    int end0;
                    if (i == workers.size() - 1)
                        end0 = nkeys;
                    else
                        end0 = (i + 1) * nkeyspertask;
                    final int end = end0;
                    Runnable r = new Runnable() {
                        @Override
                        public void run() {
                            for (int k = begin; k < end; k++) 
                                thisW.putInMC(k, rec);
                        }
                    };
                    tasks.add(r);
                }
                ExecutorService pool = Executors.newCachedThreadPool();
                for (Runnable r : tasks)
                    pool.submit(r);
                pool.shutdown();
                try {
                    pool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    LOG.warn(e);
                }
                for (int i = 0; i < workers.size(); i++) {
                    final YCSBWorker thisW = (YCSBWorker) workers.get(i);
                    thisW.getMCClient().waitForQueues(Long.MAX_VALUE, TimeUnit.SECONDS);
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
