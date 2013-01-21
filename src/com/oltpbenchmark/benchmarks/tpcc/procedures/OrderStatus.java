package com.oltpbenchmark.benchmarks.tpcc.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONStyle;
import net.minidev.json.JSONValue;

import org.apache.log4j.Logger;

import net.spy.memcached.MemcachedClient;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;
import com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Customer;
import com.oltpbenchmark.memcached.MemcachedClientIface;

public class OrderStatus extends Procedure {

    private static final Logger LOG = Logger.getLogger(OrderStatus.class);
	
	public SQLStmt ordStatGetNewestOrdSQL = new SQLStmt("SELECT o_id, o_carrier_id, o_entry_d FROM oorder"
			+ " WHERE o_w_id = ?"
			+ " AND o_d_id = ? AND o_c_id = ? ORDER BY o_id DESC LIMIT 1");
	
	public SQLStmt ordStatGetOrderLinesSQL = new SQLStmt("SELECT ol_i_id, ol_supply_w_id, ol_quantity,"
			+ " ol_amount, ol_delivery_d"
			+ " FROM order_line"
			+ " WHERE ol_o_id = ?"
			+ " AND ol_d_id =?"
			+ " AND ol_w_id = ?");
	
	public SQLStmt payGetCustSQL = new SQLStmt("SELECT c_first, c_middle, c_last, c_street_1, c_street_2, "
			+ "c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, "
			+ "c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_since FROM customer WHERE "
			+ "c_w_id = ? AND c_d_id = ? AND c_id = ?");
	
	public SQLStmt customerByNameSQL = new SQLStmt("SELECT c_first, c_middle, c_id, c_street_1, c_street_2, c_city, "
			+ "c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, "
			+ "c_balance, c_ytd_payment, c_payment_cnt, c_since FROM customer "
			+ "WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first");

	private PreparedStatement ordStatGetNewestOrd = null;
	private PreparedStatement ordStatGetOrderLines = null;
	private PreparedStatement payGetCust = null;
	private PreparedStatement customerByName = null;

  public static String MCKeyOrderLinesByOrderId(int w_id, int d_id, int o_id) {
    return "tpcc:order_line:w_id:" + w_id + ":d_id:" + d_id + ":o_id:" + o_id;
  }

  public static String MCKeyNewestOrderByCustId(int w_id, int d_id, int c_id) {
    return "tpcc:oorder:w_id:" + w_id + ":d_id:" + d_id + ":c_id:" + c_id;
  }

  public static String MCKeyCustById(int w_id, int d_id, int c_id) {
    return "tpcc:customer:w_id:" + w_id + ":d_id:" + d_id + ":c_id:" + c_id;
  }

  public static String MCKeyCustByName(int w_id, int d_id, String c_last) {
    return "tpcc:customer:w_id:" + w_id + ":d_id:" + d_id + ":c_last:" + c_last;
  }

  public static class OOrderEntry {
    public final int o_id;
    public final int o_carrier_id;
    public final Timestamp o_entry_d;

    public OOrderEntry(int o_id, int o_carrier_id, Timestamp o_entry_d) {
      this.o_id = o_id;
      this.o_carrier_id = o_carrier_id;
      this.o_entry_d = o_entry_d;
    }

    public String toJson() {
      JSONArray a = new JSONArray();
      a.add(o_id);
      a.add(o_carrier_id);
      a.add(o_entry_d.getTime());
      return a.toJSONString();
    }

    public static OOrderEntry FromJson(String s) {
      JSONArray a = (JSONArray) JSONValue.parse(s);
      if (a == null)
          throw new RuntimeException("bad json: " + s);
      return new OOrderEntry(
          (Integer)a.get(0),
          (Integer)a.get(1),
          new Timestamp((Long)a.get(2)));
    }
  }

  public static class OrderLineEntry {
    public final int ol_i_id;
    public final int ol_supply_w_id;
    public final int ol_quantity;
    public final double ol_amount;
    public final Timestamp ol_delivery_d;

    public OrderLineEntry(
      int ol_i_id,
      int ol_supply_w_id,
      int ol_quantity,
      double ol_amount,
      Timestamp ol_delivery_d) {
      this.ol_i_id = ol_i_id;
      this.ol_supply_w_id = ol_supply_w_id;
      this.ol_quantity = ol_quantity;
      this.ol_amount = ol_amount;
      this.ol_delivery_d = ol_delivery_d;
    }

    public String toJson() {
      return toJsonArray().toJSONString();
    }

    public JSONArray toJsonArray() {
      JSONArray a = new JSONArray();
      a.add(ol_i_id);
      a.add(ol_supply_w_id);
      a.add(ol_quantity);
      a.add(ol_amount);
      a.add(ol_delivery_d == null ? null : ol_delivery_d.getTime());
      return a;
    }

    public static OrderLineEntry FromJson(String s) {
      JSONArray a = (JSONArray) JSONValue.parse(s);
      if (a == null)
          throw new RuntimeException("bad json: " + s);
      return FromJsonArray(a);
    }

    public static OrderLineEntry FromJsonArray(JSONArray a) {
      return new OrderLineEntry(
          (Integer)a.get(0),
          (Integer)a.get(1),
          (Integer)a.get(2),
          (Double)a.get(3),
          a.get(4) == null ? null : new Timestamp((Long)a.get(4)));
    }
  }

	 public ResultSet run(Connection conn, MemcachedClientIface mcclient, Random gen,
				int terminalWarehouseID, int numWarehouses,
				int terminalDistrictLowerID, int terminalDistrictUpperID,
				TPCCWorker w) throws SQLException{
	
		 
			//initializing all prepared statements
			payGetCust =this.getPreparedStatement(conn, payGetCustSQL);
			customerByName=this.getPreparedStatement(conn, customerByNameSQL);
			ordStatGetNewestOrd =this.getPreparedStatement(conn, ordStatGetNewestOrdSQL);
			ordStatGetOrderLines=this.getPreparedStatement(conn, ordStatGetOrderLinesSQL);
				
     int districtID;
     if (w.getSkewGen() != null) {
       int v = w.getSkewGen().nextInt();
       terminalWarehouseID = v / jTPCCConfig.configDistPerWhse;
       districtID = v % jTPCCConfig.configDistPerWhse;
       
       // note: terminal/district are 1-indexed
       terminalWarehouseID++; districtID++;
     } else {
       districtID = TPCCUtil.randomNumber(terminalDistrictLowerID,terminalDistrictUpperID, gen);
     }

			boolean isCustomerByName=false;
			int y = TPCCUtil.randomNumber(1, 100, gen);
			String customerLastName = null;
			int customerID = -1;
			if (y <= 60) {
				isCustomerByName = true;
				customerLastName = TPCCUtil
						.getNonUniformRandomLastNameForRun(gen);
			} else {
				isCustomerByName = false;
				customerID = TPCCUtil.getCustomerID(gen);
			}

			orderStatusTransaction(terminalWarehouseID, districtID,
							customerID, customerLastName, isCustomerByName, conn, mcclient, w);
			return null;
	 }
	
	// attention duplicated code across trans... ok for now to maintain separate prepared statements
			public Customer getCustomerById(int c_w_id, int c_d_id, int c_id, Connection conn, MemcachedClientIface mcclient)
					throws SQLException {

        String mckey = null;
        if (mcclient != null) {
            Object o = null;
            if ((o = mcclient.get((mckey = MCKeyCustById(c_w_id, c_d_id, c_id)))) != null) {
                return Customer.FromJson((String) o);
            }
        }
		
				payGetCust.setInt(1, c_w_id);
				payGetCust.setInt(2, c_d_id);
				payGetCust.setInt(3, c_id);
				ResultSet rs = payGetCust.executeQuery();
				if (!rs.next()) {
					throw new RuntimeException("C_ID=" + c_id + " C_D_ID=" + c_d_id
							+ " C_W_ID=" + c_w_id + " not found!");
				}

				Customer c = TPCCUtil.newCustomerFromResults(rs);
				c.c_id = c_id;
				c.c_last = rs.getString("c_last");
				rs.close();

        if (mcclient != null) {
            try { 
                mcclient.set(mckey, jTPCCConfig.MC_KEY_TIMEOUT, c.toJson());
            } catch (IllegalStateException e) {
                LOG.warn("MC queue is full", e);
            }
        }
				return c;
			}
	
			private void orderStatusTransaction(int w_id, int d_id, int c_id,
					String c_last, boolean c_by_name, Connection conn, MemcachedClientIface mcclient, TPCCWorker w) throws SQLException {
				int o_id = -1, o_carrier_id = -1;
				Timestamp entdate = null;
				ArrayList<String> orderLines = new ArrayList<String>();

				Customer c;
				if (c_by_name) {
					assert c_id <= 0;
					// TODO: This only needs c_balance, c_first, c_middle, c_id
					// only fetch those columns?
					c = getCustomerByName(w_id, d_id, c_last, mcclient);
				} else {
					assert c_last == null;
					c = getCustomerById(w_id, d_id, c_id, conn, mcclient);
				}

				// find the newest order for the customer
				// retrieve the carrier & order date for the most recent order.

        String mckeyNewestOrder = null;
        OOrderEntry ent = null;

        if (mcclient != null) {
          Object o = null;
          if ((o = mcclient.get((mckeyNewestOrder = MCKeyNewestOrderByCustId(w_id, d_id, c.c_id)))) != null) {
            ent = OOrderEntry.FromJson((String) o);
            o_id = ent.o_id;
            o_carrier_id = ent.o_carrier_id;
            entdate = ent.o_entry_d;
          }
        }

        ResultSet rs = null;
	
        if (ent == null) {
          ordStatGetNewestOrd.setInt(1, w_id);
          ordStatGetNewestOrd.setInt(2, d_id);
          ordStatGetNewestOrd.setInt(3, c.c_id);
          rs = ordStatGetNewestOrd.executeQuery();

          if (!rs.next()) {
            throw new RuntimeException("No orders for o_w_id=" + w_id
                + " o_d_id=" + d_id + " o_c_id=" + c.c_id);
          }

          o_id = rs.getInt("o_id");
          o_carrier_id = rs.getInt("o_carrier_id");
          entdate = rs.getTimestamp("o_entry_d");
          rs.close();
          rs = null;

          if (mcclient != null) {
            try { 
              mcclient.set(mckeyNewestOrder, jTPCCConfig.MC_KEY_TIMEOUT, new OOrderEntry(o_id, o_carrier_id, entdate).toJson());
            } catch (IllegalStateException e) {
              LOG.warn("MC queue is full", e);
            }
          }
        }

				// retrieve the order lines for the most recent order
        List<OrderLineEntry> ents = null;
        String mckeyOrderLines = null;

        if (mcclient != null) {
          Object o = null;
          if ((o = mcclient.get((mckeyOrderLines = MCKeyOrderLinesByOrderId(w_id, d_id, o_id)))) != null) {
            JSONArray a = (JSONArray) JSONValue.parse((String) o);
            if (a == null)
              throw new RuntimeException("bad json: " + o);
            ents = new ArrayList<OrderLineEntry>();
            for (int i = 0; i < a.size(); i++) 
              ents.add(OrderLineEntry.FromJsonArray((JSONArray) a.get(i)));
          }
        }

        if (ents == null) {
          ents = new ArrayList<OrderLineEntry>();

          ordStatGetOrderLines.setInt(1, o_id);
          ordStatGetOrderLines.setInt(2, d_id);
          ordStatGetOrderLines.setInt(3, w_id);
          rs = ordStatGetOrderLines.executeQuery();

          while (rs.next()) {
            ents.add(new OrderLineEntry(
                  rs.getInt("ol_i_id"),  
                  rs.getInt("ol_supply_w_id"),  
                  rs.getInt("ol_quantity"),  
                  rs.getDouble("ol_amount"),  
                  rs.getTimestamp("ol_delivery_d")));
          }
          rs.close();
          rs = null;

          if (mcclient != null) {
            try { 
              JSONArray a = new JSONArray();
              for (int i = 0; i < ents.size(); i++)
                a.add(ents.get(i).toJsonArray());
              mcclient.set(mckeyOrderLines, jTPCCConfig.MC_KEY_TIMEOUT, a.toJSONString());
            } catch (IllegalStateException e) {
              LOG.warn("MC queue is full", e);
            }
          }
        }
			
				// commit the transaction
				conn.commit();

        for (OrderLineEntry oent : ents) {
          StringBuilder orderLine = new StringBuilder();
          orderLine.append("[");
          orderLine.append(oent.ol_supply_w_id);
          orderLine.append(" - ");
          orderLine.append(oent.ol_i_id);
          orderLine.append(" - ");
          orderLine.append(oent.ol_quantity);
          orderLine.append(" - ");
          orderLine.append(TPCCUtil.formattedDouble(oent.ol_amount));
          orderLine.append(" - ");
          if (oent.ol_delivery_d != null)
            orderLine.append(oent.ol_delivery_d);
          else
            orderLine.append("99-99-9999");
          orderLine.append("]");
          orderLines.add(orderLine.toString());
        }

				StringBuilder terminalMessage = new StringBuilder();
				terminalMessage.append("\n");
				terminalMessage
						.append("+-------------------------- ORDER-STATUS -------------------------+\n");
				terminalMessage.append(" Date: ");
				terminalMessage.append(TPCCUtil.getCurrentTime());
				terminalMessage.append("\n\n Warehouse: ");
				terminalMessage.append(w_id);
				terminalMessage.append("\n District:  ");
				terminalMessage.append(d_id);
				terminalMessage.append("\n\n Customer:  ");
				terminalMessage.append(c.c_id);
				terminalMessage.append("\n   Name:    ");
				terminalMessage.append(c.c_first);
				terminalMessage.append(" ");
				terminalMessage.append(c.c_middle);
				terminalMessage.append(" ");
				terminalMessage.append(c.c_last);
				terminalMessage.append("\n   Balance: ");
				terminalMessage.append(c.c_balance);
				terminalMessage.append("\n\n");
				if (o_id == -1) {
					terminalMessage.append(" Customer has no orders placed.\n");
				} else {
					terminalMessage.append(" Order-Number: ");
					terminalMessage.append(o_id);
					terminalMessage.append("\n    Entry-Date: ");
					terminalMessage.append(entdate);
					terminalMessage.append("\n    Carrier-Number: ");
					terminalMessage.append(o_carrier_id);
					terminalMessage.append("\n\n");
					if (orderLines.size() != 0) {
						terminalMessage
								.append(" [Supply_W - Item_ID - Qty - Amount - Delivery-Date]\n");
						for (String orderLine : orderLines) {
							terminalMessage.append(" ");
							terminalMessage.append(orderLine);
							terminalMessage.append("\n");
						}
					} else {
					    if(LOG.isTraceEnabled()) LOG.trace(" This Order has no Order-Lines.\n");
					}
				}
				terminalMessage.append("+-----------------------------------------------------------------+\n\n");
				if(LOG.isTraceEnabled()) LOG.trace(terminalMessage.toString());
			}
			
			//attention this code is repeated in other transacitons... ok for now to allow for separate statements.
			public Customer getCustomerByName(int c_w_id, int c_d_id, String c_last, MemcachedClientIface mcclient)
					throws SQLException {

        String mckey = null;
        if (mcclient != null) {
            Object o = null;
            if ((o = mcclient.get((mckey = MCKeyCustByName(c_w_id, c_d_id, c_last)))) != null) {
                return Customer.FromJson((String) o);
            }
        }

				ArrayList<Customer> customers = new ArrayList<Customer>();
	
				customerByName.setInt(1, c_w_id);
				customerByName.setInt(2, c_d_id);
				customerByName.setString(3, c_last);
				ResultSet rs = customerByName.executeQuery();

				while (rs.next()) {
					Customer c = TPCCUtil.newCustomerFromResults(rs);
					c.c_id = rs.getInt("c_id");
					c.c_last = c_last;
					customers.add(c);
				}
				rs.close();

				if (customers.size() == 0) {
					throw new RuntimeException("C_LAST=" + c_last + " C_D_ID=" + c_d_id
							+ " C_W_ID=" + c_w_id + " not found!");
				}

				// TPC-C 2.5.2.2: Position n / 2 rounded up to the next integer, but
				// that
				// counts starting from 1.
				int index = customers.size() / 2;
				if (customers.size() % 2 == 0) {
					index -= 1;
				}
				Customer ret = customers.get(index);

        if (mcclient != null) {
            try { 
                mcclient.set(mckey, jTPCCConfig.MC_KEY_TIMEOUT, ret.toJson());
            } catch (IllegalStateException e) {
                LOG.warn("MC queue is full", e);
            }
        }
        return ret;
			}

  public static void main(String[] args) {
    OrderLineEntry[] ents = new OrderLineEntry[2];
    ents[0] = new OrderLineEntry(1, 1, 1, 1.0, null);
    ents[1] = new OrderLineEntry(2, 2, 2, 2.0, null);
    JSONArray a = new JSONArray();
    for (int i = 0; i < ents.length; i++)
      a.add(ents[i].toJsonArray());
    System.out.println(a.toJSONString());
  }
}
