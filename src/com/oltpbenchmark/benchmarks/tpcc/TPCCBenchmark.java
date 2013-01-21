/*******************************************************************************
 * oltpbenchmark.com
 *  
 *  Project Info:  http://oltpbenchmark.com
 *  Project Members:  	Carlo Curino <carlo.curino@gmail.com>
 * 				Evan Jones <ej@evanjones.ca>
 * 				DIFALLAH Djellel Eddine <djelleleddine.difallah@unifr.ch>
 * 				Andy Pavlo <pavlo@cs.brown.edu>
 * 				CUDRE-MAUROUX Philippe <philippe.cudre-mauroux@unifr.ch>  
 *  				Yang Zhang <yaaang@gmail.com> 
 * 
 *  This library is free software; you can redistribute it and/or modify it under the terms
 *  of the GNU General Public License as published by the Free Software Foundation;
 *  either version 3.0 of the License, or (at your option) any later version.
 * 
 *  This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Lesser General Public License for more details.
 ******************************************************************************/
package com.oltpbenchmark.benchmarks.tpcc;

import static com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig.terminalPrefix;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Customer;
import com.oltpbenchmark.benchmarks.tpcc.procedures.NewOrder;
import com.oltpbenchmark.benchmarks.tpcc.procedures.OrderStatus;
import com.oltpbenchmark.memcached.MemcachedClientIface;
import com.oltpbenchmark.util.SimpleSystemPrinter;


public class TPCCBenchmark extends BenchmarkModule {
    private static final Logger LOG = Logger.getLogger(TPCCBenchmark.class);

  public static class SkewDesc {
    public final double hotAccessSkew;
    public final double hotDataSkew;
    public final double warmAccessSkew;
    public final double warmDataSkew;
    public SkewDesc(double hotAccessSkew,
                    double hotDataSkew,
                    double warmAccessSkew,
                    double warmDataSkew) {
      this.hotAccessSkew = hotAccessSkew;
      this.hotDataSkew = hotDataSkew;
      this.warmAccessSkew = warmAccessSkew;
      this.warmDataSkew = warmDataSkew;
    }
  }

  private final SkewDesc skewDesc;
  
	public TPCCBenchmark(WorkloadConfiguration workConf) {
		super("tpcc", workConf, true);
      XMLConfiguration xml = workConf.getXmlConfig();
      if (xml != null && xml.getBoolean("useSkew")) {
        skewDesc = new SkewDesc(
          xml.getDouble("hotAccessSkew"),
          xml.getDouble("hotDataSkew"),
          xml.getDouble("warmAccessSkew"),
          xml.getDouble("warmDataSkew"));
      } else {
        skewDesc = null;
      }
	}

	@Override
	protected Package getProcedurePackageImpl() {
		return (NewOrder.class.getPackage());
	}

  private static abstract class WarmupRunnable implements Runnable {
    protected final Connection conn;
    protected final MemcachedClientIface mcclient;
    public WarmupRunnable(Connection conn, MemcachedClientIface mcclient) {
      this.conn = conn;
      this.mcclient = mcclient;
    }
    @Override
    public void run() {
      Statement stmt = null;
      try {
        stmt = conn.createStatement();
        doWork(stmt);
      } catch (SQLException e) {
        if (stmt != null) {
          try {
            stmt.close();
          } catch (SQLException e1) {
            LOG.error("exception while trying to close stmt b/c of exception", e1);
          }
        }
      }
    }

    protected abstract void doWork(Statement stmt) throws SQLException;
  }

  private static class ItemByIdWarmup extends WarmupRunnable {
    public ItemByIdWarmup(Connection conn, MemcachedClientIface mcclient) {
      super(conn, mcclient);
    }
    protected void doWork(Statement stmt) throws SQLException {
      System.err.println("warming up ItemById");
      ResultSet rs = stmt.executeQuery("SELECT i_id, i_price, i_name, i_data FROM item"); 
      while (rs.next()) {
        NewOrder.ItemEntry ent = new NewOrder.ItemEntry(rs.getFloat("i_price"), rs.getString("i_name"), rs.getString("i_data"));
        String mckey = NewOrder.MCKeyItemById(rs.getInt("i_id"));
        try { 
          mcclient.set(mckey, jTPCCConfig.MC_KEY_TIMEOUT, ent.toJson());
        } catch (IllegalStateException e) {
          LOG.warn("MC queue is full", e);
        }
      }
      rs.close();
    }
  }

  private static class CustWarehouseJoinWarmup extends WarmupRunnable {
    public CustWarehouseJoinWarmup(Connection conn, MemcachedClientIface mcclient) {
      super(conn, mcclient);
    }
    protected void doWork(Statement stmt) throws SQLException {
      System.err.println("warming up CustWarehouseJoin");
      ResultSet rs = stmt.executeQuery("SELECT w_id, c_d_id, c_id, c_discount, c_last, c_credit, w_tax"
          + "  FROM customer, warehouse"
          + " WHERE w_id = c_w_id");
      while (rs.next()) {
        NewOrder.CustWarehouseEntry ent = new NewOrder.CustWarehouseEntry(
            rs.getFloat("c_discount"), rs.getString("c_last"), 
            rs.getString("c_credit"), rs.getFloat("w_tax"));
        String mckey = NewOrder.MCKeyCustWarehouseJoin(rs.getInt("w_id"), rs.getInt("c_d_id"), rs.getInt("c_id"));
        try { 
          mcclient.set(mckey, jTPCCConfig.MC_KEY_TIMEOUT, ent.toJson());
        } catch (IllegalStateException e) {
          LOG.warn("MC queue is full", e);
        }
      }
      rs.close();
    }
  }

  private static class NewestOrderWarmup extends WarmupRunnable {
    public NewestOrderWarmup(Connection conn, MemcachedClientIface mcclient) {
      super(conn, mcclient);
    }
    protected void doWork(Statement stmt) throws SQLException {
      System.err.println("warming up NewestOrder");
      // find the latest order id for each unique (w_id, d_id, c_id) tuple
      ResultSet rs = stmt.executeQuery(
          "select b.o_w_id, b.o_d_id, b.o_c_id, b.o_id, b.o_carrier_id, b.o_entry_d " + 
          "from (select o_w_id, o_d_id, o_c_id, max(o_id) as max_o_id from oorder group by o_w_id, o_d_id, o_c_id) as a, oorder as b " + 
          "where a.o_w_id = b.o_w_id and a.o_d_id = b.o_d_id and a.o_c_id = b.o_c_id and a.max_o_id = b.o_id;");
      while (rs.next()) {
        OrderStatus.OOrderEntry ent = new OrderStatus.OOrderEntry(rs.getInt("o_id"), rs.getInt("o_carrier_id"), rs.getTimestamp("o_entry_d"));
        String mckey = OrderStatus.MCKeyNewestOrderByCustId(rs.getInt("o_w_id"), rs.getInt("o_d_id"), rs.getInt("o_c_id"));
        try { 
          mcclient.set(mckey, jTPCCConfig.MC_KEY_TIMEOUT, ent.toJson());
        } catch (IllegalStateException e) {
          LOG.warn("MC queue is full", e);
        }
      }
      rs.close();
    }
  }

  private static class CustByIdWarmup extends WarmupRunnable {
    public CustByIdWarmup(Connection conn, MemcachedClientIface mcclient) {
      super(conn, mcclient);
    }
    protected void doWork(Statement stmt) throws SQLException {
      System.err.println("warming up CustByID");
      ResultSet rs = stmt.executeQuery("select * from customer;");
      while (rs.next()) {
        Customer c = TPCCUtil.newCustomerFromResults(rs);
        c.c_id = rs.getInt("c_id");
        c.c_last = rs.getString("c_last");
        String mckey = OrderStatus.MCKeyCustById(rs.getInt("c_w_id"), rs.getInt("c_d_id"), rs.getInt("c_id"));
        try { 
          mcclient.set(mckey, jTPCCConfig.MC_KEY_TIMEOUT, c.toJson());
        } catch (IllegalStateException e) {
          LOG.warn("MC queue is full", e);
        }
      }
      rs.close();
    }
  }

  private static class CustByNameWarmup extends WarmupRunnable {
    public CustByNameWarmup(Connection conn, MemcachedClientIface mcclient) {
      super(conn, mcclient);
    }
    protected void doWork(Statement stmt) throws SQLException {
      System.err.println("warming up CustByName");
      // a bit of hackery in mysql
      ResultSet rs = stmt.executeQuery(
          "select m.c_w_id as key0, m.c_d_id as key1, m.c_last as key2, cust.* from " + 
          "(select c_w_id, c_d_id, c_last, substring_index(substring_index(ids, ',', idx + 1), ',', -1) as c_id from " + 
          "(select c_w_id, c_d_id, c_last, group_concat(c_id) as ids, " + 
          "if ((count(*) % 2) = 0, floor(count(*) / 2) - 1, floor(count(*) / 2)) as idx from " + 
          "customer group by c_w_id, c_d_id, c_last) as r) as m, customer cust " + 
          "where m.c_w_id = cust.c_w_id and m.c_d_id = cust.c_d_id and m.c_id = cust.c_id");
      while (rs.next()) {
        Customer c = TPCCUtil.newCustomerFromResults(rs);
        c.c_id = rs.getInt("c_id");
        c.c_last = rs.getString("c_last");
        String mckey = OrderStatus.MCKeyCustByName(rs.getInt("key0"), rs.getInt("key1"), rs.getString("key2"));
        try { 
          mcclient.set(mckey, jTPCCConfig.MC_KEY_TIMEOUT, c.toJson());
        } catch (IllegalStateException e) {
          LOG.warn("MC queue is full", e);
        }
      }
      rs.close();
    }
  }

	/**
	 * @param Bool
	 */
	@Override
	protected List<Worker> makeWorkersImpl(boolean verbose) throws IOException {
		// HACK: Turn off terminal messages
		jTPCCConfig.TERMINAL_MESSAGES = false;
		ArrayList<Worker> workers = new ArrayList<Worker>();

		try {
			List<TPCCWorker> terminals = createTerminals();
			workers.addAll(terminals);
		} catch (Exception e) {
			e.printStackTrace();
		}

    if (((TPCCWorker)workers.get(0)).getMCClient() != null) {
      System.err.println("warming up memcached");
      if (workers.size() >= 5) {
        List<WarmupRunnable> runners = new ArrayList<WarmupRunnable>();
        runners.add(new ItemByIdWarmup(((TPCCWorker)workers.get(0)).getConnection(), ((TPCCWorker)workers.get(0)).getMCClient()));
        runners.add(new CustWarehouseJoinWarmup(((TPCCWorker)workers.get(1)).getConnection(), ((TPCCWorker)workers.get(1)).getMCClient()));
        runners.add(new NewestOrderWarmup(((TPCCWorker)workers.get(2)).getConnection(), ((TPCCWorker)workers.get(2)).getMCClient()));
        runners.add(new CustByIdWarmup(((TPCCWorker)workers.get(3)).getConnection(), ((TPCCWorker)workers.get(3)).getMCClient()));
        runners.add(new CustByNameWarmup(((TPCCWorker)workers.get(4)).getConnection(), ((TPCCWorker)workers.get(4)).getMCClient()));

        ExecutorService exec = Executors.newCachedThreadPool();
        List<Future<?>> futures = new ArrayList<Future<?>>();
        for (WarmupRunnable r : runners)
          futures.add(exec.submit(r));
        for (Future<?> f : futures) {
          while (true) {
            try {
              f.get();
              break;
            } catch (InterruptedException e) {
              LOG.warn("interrupted while waiting", e);
            } catch (ExecutionException e) {
              LOG.error("execution exception while waiting", e);
            }
          }
        }
        exec.shutdownNow();
      } else {
        LOG.warn("skipping mc warmup because not enough workers");
      }
    }
		return workers;
	}

	@Override
	protected Loader makeLoaderImpl(Connection conn) throws SQLException {
		return new TPCCLoader(this, conn);
	}

	protected ArrayList<TPCCWorker> createTerminals() throws SQLException {

		TPCCWorker[] terminals = new TPCCWorker[workConf.getTerminals()];

		int numWarehouses = (int) workConf.getScaleFactor();//tpccConf.getNumWarehouses();
		int numTerminals = workConf.getTerminals();
		assert (numTerminals >= numWarehouses) :
		    String.format("Insufficient number of terminals '%d' [numWarehouses=%d]",
		                  numTerminals, numWarehouses);

		String[] terminalNames = new String[numTerminals];
		// TODO: This is currently broken: fix it!
		int warehouseOffset = Integer.getInteger("warehouseOffset", 1);
		assert warehouseOffset == 1;

		// We distribute terminals evenly across the warehouses
		// Eg. if there are 10 terminals across 7 warehouses, they
		// are distributed as
		// 1, 1, 2, 1, 2, 1, 2
		final double terminalsPerWarehouse = (double) numTerminals
				/ numWarehouses;
		assert terminalsPerWarehouse >= 1;
		for (int w = 0; w < numWarehouses; w++) {
			// Compute the number of terminals in *this* warehouse
			int lowerTerminalId = (int) (w * terminalsPerWarehouse);
			int upperTerminalId = (int) ((w + 1) * terminalsPerWarehouse);
			// protect against double rounding errors
			int w_id = w + 1;
			if (w_id == numWarehouses)
				upperTerminalId = numTerminals;
			int numWarehouseTerminals = upperTerminalId - lowerTerminalId;

			LOG.info(String.format("w_id %d = %d terminals [lower=%d / upper%d]",
			                       w_id, numWarehouseTerminals, lowerTerminalId, upperTerminalId));

			final double districtsPerTerminal = jTPCCConfig.configDistPerWhse
					/ (double) numWarehouseTerminals;
			assert districtsPerTerminal >= 1 :
			    String.format("Too many terminals [districtsPerTerminal=%.2f, numWarehouseTerminals=%d]",
			                  districtsPerTerminal, numWarehouseTerminals);
			for (int terminalId = 0; terminalId < numWarehouseTerminals; terminalId++) {
				int lowerDistrictId = (int) (terminalId * districtsPerTerminal);
				int upperDistrictId = (int) ((terminalId + 1) * districtsPerTerminal);
				if (terminalId + 1 == numWarehouseTerminals) {
					upperDistrictId = jTPCCConfig.configDistPerWhse;
				}
				lowerDistrictId += 1;

				String terminalName = terminalPrefix + "w" + w_id + "d"
						+ lowerDistrictId + "-" + upperDistrictId;

				TPCCWorker terminal = new TPCCWorker(terminalName, w_id,
						lowerDistrictId, upperDistrictId, this,
						new SimpleSystemPrinter(null), new SimpleSystemPrinter(
								System.err), numWarehouses, skewDesc);
				terminals[lowerTerminalId + terminalId] = terminal;
				terminalNames[lowerTerminalId + terminalId] = terminalName;
			}

		}
		assert terminals[terminals.length - 1] != null;

		ArrayList<TPCCWorker> ret = new ArrayList<TPCCWorker>();
		for (TPCCWorker w : terminals)
			ret.add(w);
		return ret;
	}

}
