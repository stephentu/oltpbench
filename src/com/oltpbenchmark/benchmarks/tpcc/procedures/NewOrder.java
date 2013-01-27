package com.oltpbenchmark.benchmarks.tpcc.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONStyle;
import net.minidev.json.JSONValue;

import net.spy.memcached.MemcachedClient;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;
import com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig;
import com.oltpbenchmark.memcached.MemcachedClientIface;

public class NewOrder extends Procedure {

    private static final Logger LOG = Logger.getLogger(NewOrder.class);

    public final SQLStmt stmtGetCustWhseSQL = new SQLStmt(
            "SELECT c_discount, c_last, c_credit, w_tax"
                    + "  FROM customer, warehouse"
                    + " WHERE w_id = ? AND c_w_id = ?"
                    + " AND c_d_id = ? AND c_id = ?");

    public final SQLStmt stmtGetDistSQL = new SQLStmt(
            "SELECT d_next_o_id, d_tax FROM district"
                    + " WHERE d_w_id = ? AND d_id = ? FOR UPDATE"
            );

    public final SQLStmt  stmtInsertNewOrderSQL = new SQLStmt("INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES ( ?, ?, ?)");

    public final SQLStmt  stmtUpdateDistSQL = new SQLStmt("UPDATE district SET d_next_o_id = d_next_o_id + 1 WHERE d_w_id = ? AND d_id = ?");

    public final SQLStmt  stmtInsertOOrderSQL = new SQLStmt("INSERT INTO oorder "
            + " (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)"
            + " VALUES (?, ?, ?, ?, ?, ?, ?)");

    public final SQLStmt  stmtGetItemSQL = new SQLStmt("SELECT i_price, i_name , i_data FROM item WHERE i_id = ?");

    public final SQLStmt  stmtGetStockSQL = new SQLStmt("SELECT s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, "
            + "       s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10"
            + " FROM stock WHERE s_i_id = ? AND s_w_id = ? FOR UPDATE");

    public final SQLStmt  stmtUpdateStockSQL = new SQLStmt("UPDATE stock SET s_quantity = ? , s_ytd = s_ytd + ?, s_remote_cnt = s_remote_cnt + ? "
            + " WHERE s_i_id = ? AND s_w_id = ?");

    public final SQLStmt  stmtInsertOrderLineSQL = new SQLStmt("INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id,"
            + "  ol_quantity, ol_amount, ol_dist_info) VALUES (?,?,?,?,?,?,?,?,?)");


    public static String MCKeyCustWarehouseJoin(int w_id, int d_id, int c_id) {
        return "tpcc:warehouse:customer:w_id:" + w_id + ":d_id:" + d_id + ":c_id:" + c_id;
    }

    public static String MCKeyItemById(int i_id) {
        return "tpcc:item:i_id:" + i_id;
    }

    public static class CustWarehouseEntry {
        public final float c_discount; 
        public final String c_last;
        public final String c_credit;
        public final float w_tax;

        public CustWarehouseEntry(float c_discount, String c_last, String c_credit, float w_tax) {
            this.c_discount = c_discount; 
            this.c_last = c_last;
            this.c_credit = c_credit;
            this.w_tax = w_tax;
        }

        public String toJson() {
            JSONArray a = new JSONArray();
            a.add(c_discount);
            a.add(c_last);
            a.add(c_credit);
            a.add(w_tax);
            return a.toJSONString();
        }

        public static CustWarehouseEntry FromJson(String s) {
            JSONArray a = (JSONArray) JSONValue.parse(s);
            if (a == null)
                throw new RuntimeException("bad json: " + s);
            return new CustWarehouseEntry(
                    ((Double)a.get(0)).floatValue(),
                    (String)a.get(1),
                    (String)a.get(2),
                    ((Double)a.get(3)).floatValue());
        }
    }

    public static class ItemEntry {
        public final float i_price;
        public final String i_name;
        public final String i_data;

        public ItemEntry(float i_price, String i_name, String i_data) {
            this.i_price = i_price;
            this.i_name = i_name;
            this.i_data = i_data;
        }

        public String toJson() {
            JSONArray a = new JSONArray();
            a.add(i_price);
            a.add(i_name);
            a.add(i_data);
            return a.toJSONString();
        }

        public static ItemEntry FromJson(String s) {
            JSONArray a = (JSONArray) JSONValue.parse(s);
            if (a == null)
                throw new RuntimeException("bad json: " + s);
            return new ItemEntry(
                    ((Double)a.get(0)).floatValue(),
                    (String)a.get(1),
                    (String)a.get(2));
        }
    }

    // NewOrder Txn
    private PreparedStatement stmtGetCustWhse = null; 
    private PreparedStatement stmtGetDist = null;
    private PreparedStatement stmtInsertNewOrder = null;
    private PreparedStatement stmtUpdateDist = null;
    private PreparedStatement stmtInsertOOrder = null;
    private PreparedStatement stmtGetItem = null;
    private PreparedStatement stmtGetStock = null;
    private PreparedStatement stmtUpdateStock = null;
    private PreparedStatement stmtInsertOrderLine = null;

    private static class Pair<A extends Comparable<A>, B extends Comparable<B>> implements Comparable<Pair<A, B>> {
        public final A a;
        public final B b;

        public Pair(A a, B b) {
            this.a = a;
            this.b = b;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null)
                return false;
            if (!(o instanceof Pair<?, ?>))
                return false;
            Pair<A, B> that = (Pair<A, B>) o;
            return this.a.equals(that.a) && this.b.equals(that.b);
        }

        @Override
        public int compareTo(Pair<A, B> that) {
            int acmp = this.a.compareTo(that.a);
            if (acmp < 0)
                return -1;
            if (acmp > 0)
                return 1;
            return this.b.compareTo(that.b);
        }
    }

    public ResultSet run(Connection conn, MemcachedClientIface mcclient, Random gen,
            int terminalWarehouseID, int numWarehouses,
            int terminalDistrictLowerID, int terminalDistrictUpperID,
            TPCCWorker w) throws SQLException {

        //initializing all prepared statements
        stmtGetCustWhse=this.getPreparedStatement(conn, stmtGetCustWhseSQL);
        stmtGetDist=this.getPreparedStatement(conn, stmtGetDistSQL);
        stmtInsertNewOrder=this.getPreparedStatement(conn, stmtInsertNewOrderSQL);
        stmtUpdateDist =this.getPreparedStatement(conn, stmtUpdateDistSQL);
        stmtInsertOOrder =this.getPreparedStatement(conn, stmtInsertOOrderSQL);
        stmtGetItem =this.getPreparedStatement(conn, stmtGetItemSQL);
        stmtGetStock =this.getPreparedStatement(conn, stmtGetStockSQL);
        stmtUpdateStock =this.getPreparedStatement(conn, stmtUpdateStockSQL);
        stmtInsertOrderLine =this.getPreparedStatement(conn, stmtInsertOrderLineSQL);

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

        int customerID = TPCCUtil.getCustomerID(gen);

        int numItems = (int) TPCCUtil.randomNumber(5, 15, gen);
        int[] itemIDs = new int[numItems];
        int[] supplierWarehouseIDs = new int[numItems];
        int[] orderQuantities = new int[numItems];
        List<Pair<Integer, Integer>> values = new ArrayList<Pair<Integer, Integer>>();
        int allLocal = 1;
        for (int i = 0; i < numItems; i++) {
            int item = TPCCUtil.getItemID(gen);
            int warehouse;

            if (w.getSkewGen() != null || TPCCUtil.randomNumber(1, 100, gen) > 1) {
                warehouse = terminalWarehouseID;
            } else {
                do {
                    warehouse = TPCCUtil.randomNumber(1,
                            numWarehouses, gen);
                } while (warehouse == terminalWarehouseID
                        && numWarehouses > 1);
                allLocal = 0;
            }

            values.add(new Pair<Integer, Integer>(warehouse, item));
            orderQuantities[i] = TPCCUtil.randomNumber(1, 10, gen);
        }

        // we need to cause 1% of the new orders to be rolled back.
        if (TPCCUtil.randomNumber(1, 100, gen) == 1)
            values.set(
                    numItems - 1, 
                    new Pair<Integer, Integer>(values.get(numItems - 1).a, jTPCCConfig.INVALID_ITEM_ID));

        // induce a sort ordering for items, to avoid deadlock
        // use the ordering: (warehouse_id, item_id)
        Collections.sort(values);

        for (int i = 0; i < numItems; i++) {
            supplierWarehouseIDs[i] = values.get(i).a;
            itemIDs[i] = values.get(i).b;
        }

        newOrderTransaction(terminalWarehouseID, districtID,
                customerID, numItems, allLocal, itemIDs,
                supplierWarehouseIDs, orderQuantities, conn, mcclient, w);
        return null;

    }

    private void newOrderTransaction(int w_id, int d_id, int c_id,
            int o_ol_cnt, int o_all_local, int[] itemIDs,
            int[] supplierWarehouseIDs, int[] orderQuantities, Connection conn, MemcachedClientIface mcclient, TPCCWorker w)
                    throws SQLException {
        float c_discount = 0, w_tax = 0, d_tax = 0, i_price = 0;
        int d_next_o_id, o_id = -1, s_quantity;
        String c_last = null, c_credit = null, i_name = null, i_data = null, s_data;
        String s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05;
        String s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, ol_dist_info = null;
        float[] itemPrices = new float[o_ol_cnt];
        float[] orderLineAmounts = new float[o_ol_cnt];
        String[] itemNames = new String[o_ol_cnt];
        int[] stockQuantities = new int[o_ol_cnt];
        char[] brandGeneric = new char[o_ol_cnt];
        int ol_supply_w_id, ol_i_id, ol_quantity;
        int s_remote_cnt_increment;
        float ol_amount, total_amount = 0;
        try
        {
            CustWarehouseEntry cwEntry = null;
            String mckeyCwEntry = null;
            if (mcclient != null) {
                Object o = null;
                if ((o = mcclient.get((mckeyCwEntry = MCKeyCustWarehouseJoin(w_id, d_id, c_id)))) != null) {
                    cwEntry = CustWarehouseEntry.FromJson((String) o);
                    c_discount = cwEntry.c_discount;
                    c_last = cwEntry.c_last;
                    c_credit = cwEntry.c_credit;
                    w_tax = cwEntry.w_tax;
                }
            }

            ResultSet rs = null;
            if (cwEntry == null) {
                stmtGetCustWhse.setInt(1, w_id);
                stmtGetCustWhse.setInt(2, w_id);
                stmtGetCustWhse.setInt(3, d_id);
                stmtGetCustWhse.setInt(4, c_id);
                rs = stmtGetCustWhse.executeQuery();
                if (!rs.next())
                    throw new RuntimeException("W_ID=" + w_id + " C_D_ID=" + d_id
                            + " C_ID=" + c_id + " not found!");
                c_discount = rs.getFloat("c_discount");
                c_last = rs.getString("c_last");
                c_credit = rs.getString("c_credit");
                w_tax = rs.getFloat("w_tax");
                rs.close();
                rs = null;

                if (mcclient != null) {
                    try { 
                        mcclient.set(mckeyCwEntry, jTPCCConfig.MC_KEY_TIMEOUT, new CustWarehouseEntry(c_discount, c_last, c_credit, w_tax).toJson());
                    } catch (IllegalStateException e) {
                        LOG.warn("MC queue is full", e);
                    }
                }
            }

            stmtGetDist.setInt(1, w_id);
            stmtGetDist.setInt(2, d_id);
            rs = stmtGetDist.executeQuery();
            if (!rs.next()) {
                throw new RuntimeException("D_ID=" + d_id + " D_W_ID=" + w_id
                        + " not found!");
            }
            d_next_o_id = rs.getInt("d_next_o_id");
            d_tax = rs.getFloat("d_tax");
            rs.close();
            rs = null;
            o_id = d_next_o_id;

            stmtInsertNewOrder.setInt(1, o_id);
            stmtInsertNewOrder.setInt(2, d_id);
            stmtInsertNewOrder.setInt(3, w_id);
            stmtInsertNewOrder.executeUpdate();

            stmtUpdateDist.setInt(1, w_id);
            stmtUpdateDist.setInt(2, d_id);
            int result = stmtUpdateDist.executeUpdate();
            if (result == 0)
                throw new RuntimeException(
                        "Error!! Cannot update next_order_id on district for D_ID="
                                + d_id + " D_W_ID=" + w_id);

            // XXX: disable caching for newest order, because it only helps OrderStatus, which 
            // only runs 4% of the time, whereas NewOrder runs ~40% of the time
//            if (mcclient != null) {
//                while (true) {
//                    try {
//                        mcclient.delete(OrderStatus.MCKeyNewestOrderByCustId(w_id, d_id, c_id));
//                        break;
//                    } catch (IllegalStateException e) {
//                        LOG.warn("MC queue is full", e);
//                    }
//                }
//            }

            stmtInsertOOrder.setInt(1, o_id);
            stmtInsertOOrder.setInt(2, d_id);
            stmtInsertOOrder.setInt(3, w_id);
            stmtInsertOOrder.setInt(4, c_id);
            stmtInsertOOrder.setTimestamp(5,
                    new Timestamp(System.currentTimeMillis()));
            stmtInsertOOrder.setInt(6, o_ol_cnt);
            stmtInsertOOrder.setInt(7, o_all_local);
            stmtInsertOOrder.executeUpdate();

            for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
                ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
                ol_i_id = itemIDs[ol_number - 1];
                ol_quantity = orderQuantities[ol_number - 1];

                String mckeyItem = null;
                ItemEntry itemEntry = null;
                if (mcclient != null) {
                    Object o = null;
                    if ((o = mcclient.get((mckeyItem = MCKeyItemById(ol_i_id)))) != null) {
                        itemEntry = ItemEntry.FromJson((String) o);
                        i_price = itemEntry.i_price;
                        i_name = itemEntry.i_name;
                        i_data = itemEntry.i_data;
                    }
                }

                if (itemEntry == null) {
                    stmtGetItem.setInt(1, ol_i_id);
                    rs = stmtGetItem.executeQuery();
                    if (!rs.next()) {
                        // This is (hopefully) an expected error: this is an
                        // expected new order rollback
                        assert ol_number == o_ol_cnt;
                        assert ol_i_id == jTPCCConfig.INVALID_ITEM_ID;
                        rs.close();
                        throw new UserAbortException(
                                "EXPECTED new order rollback: I_ID=" + ol_i_id
                                + " not found!");
                    }

                    i_price = rs.getFloat("i_price");
                    i_name = rs.getString("i_name");
                    i_data = rs.getString("i_data");
                    rs.close();
                    rs = null;

                    if (mcclient != null) {
                        try { 
                            mcclient.set(mckeyItem, jTPCCConfig.MC_KEY_TIMEOUT, new ItemEntry(i_price, i_name, i_data).toJson());
                        } catch (IllegalStateException e) {
                            LOG.warn("MC queue is full", e);
                        }
                    }
                }

                itemPrices[ol_number - 1] = i_price;
                itemNames[ol_number - 1] = i_name;


                stmtGetStock.setInt(1, ol_i_id);
                stmtGetStock.setInt(2, ol_supply_w_id);
                rs = stmtGetStock.executeQuery();
                if (!rs.next())
                    throw new RuntimeException("I_ID=" + ol_i_id
                            + " not found!");
                s_quantity = rs.getInt("s_quantity");
                s_data = rs.getString("s_data");
                s_dist_01 = rs.getString("s_dist_01");
                s_dist_02 = rs.getString("s_dist_02");
                s_dist_03 = rs.getString("s_dist_03");
                s_dist_04 = rs.getString("s_dist_04");
                s_dist_05 = rs.getString("s_dist_05");
                s_dist_06 = rs.getString("s_dist_06");
                s_dist_07 = rs.getString("s_dist_07");
                s_dist_08 = rs.getString("s_dist_08");
                s_dist_09 = rs.getString("s_dist_09");
                s_dist_10 = rs.getString("s_dist_10");
                rs.close();
                rs = null;

                stockQuantities[ol_number - 1] = s_quantity;

                if (s_quantity - ol_quantity >= 10) {
                    s_quantity -= ol_quantity;
                } else {
                    s_quantity += -ol_quantity + 91;
                }

                if (ol_supply_w_id == w_id) {
                    s_remote_cnt_increment = 0;
                } else {
                    s_remote_cnt_increment = 1;
                }


                stmtUpdateStock.setInt(1, s_quantity);
                stmtUpdateStock.setInt(2, ol_quantity);
                stmtUpdateStock.setInt(3, s_remote_cnt_increment);
                stmtUpdateStock.setInt(4, ol_i_id);
                stmtUpdateStock.setInt(5, ol_supply_w_id);
                stmtUpdateStock.addBatch();

                ol_amount = ol_quantity * i_price;
                orderLineAmounts[ol_number - 1] = ol_amount;
                total_amount += ol_amount;

                if (i_data.indexOf("GENERIC") != -1
                        && s_data.indexOf("GENERIC") != -1) {
                    brandGeneric[ol_number - 1] = 'B';
                } else {
                    brandGeneric[ol_number - 1] = 'G';
                }

                switch ((int) d_id) {
                    case 1:
                        ol_dist_info = s_dist_01;
                        break;
                    case 2:
                        ol_dist_info = s_dist_02;
                        break;
                    case 3:
                        ol_dist_info = s_dist_03;
                        break;
                    case 4:
                        ol_dist_info = s_dist_04;
                        break;
                    case 5:
                        ol_dist_info = s_dist_05;
                        break;
                    case 6:
                        ol_dist_info = s_dist_06;
                        break;
                    case 7:
                        ol_dist_info = s_dist_07;
                        break;
                    case 8:
                        ol_dist_info = s_dist_08;
                        break;
                    case 9:
                        ol_dist_info = s_dist_09;
                        break;
                    case 10:
                        ol_dist_info = s_dist_10;
                        break;
                }

                stmtInsertOrderLine.setInt(1, o_id);
                stmtInsertOrderLine.setInt(2, d_id);
                stmtInsertOrderLine.setInt(3, w_id);
                stmtInsertOrderLine.setInt(4, ol_number);
                stmtInsertOrderLine.setInt(5, ol_i_id);
                stmtInsertOrderLine.setInt(6, ol_supply_w_id);
                stmtInsertOrderLine.setInt(7, ol_quantity);
                stmtInsertOrderLine.setFloat(8, ol_amount);
                stmtInsertOrderLine.setString(9, ol_dist_info);
                stmtInsertOrderLine.addBatch();

            } // end-for

            stmtInsertOrderLine.executeBatch();
            stmtUpdateStock.executeBatch();

            total_amount *= (1 + w_tax + d_tax) * (1 - c_discount);
        } catch(UserAbortException userEx)
        {
            LOG.debug("Caught an expected error in New Order");
            throw userEx;
        }
        finally {
            if (stmtInsertOrderLine != null)
                stmtInsertOrderLine.clearBatch();
            if (stmtUpdateStock != null)
                stmtUpdateStock.clearBatch();
        }

    }

}
