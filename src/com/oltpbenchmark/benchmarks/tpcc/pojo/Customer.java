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
package com.oltpbenchmark.benchmarks.tpcc.pojo;

import java.sql.Timestamp;

import net.minidev.json.JSONObject;
import net.minidev.json.JSONStyle;
import net.minidev.json.JSONValue;

public class Customer {

	public int c_id;
	public int c_d_id;
	public int c_w_id;
	public int c_payment_cnt;
	public int c_delivery_cnt;
	public Timestamp c_since;
	public float c_discount;
	public float c_credit_lim;
	public float c_balance;
	public float c_ytd_payment;
	public String c_credit;
	public String c_last;
	public String c_first;
	public String c_street_1;
	public String c_street_2;
	public String c_city;
	public String c_state;
	public String c_zip;
	public String c_phone;
	public String c_middle;
	public String c_data;

  public String toJson() {
    JSONObject obj = new JSONObject();

    obj.put("c_id"           , new Integer(c_id));
    obj.put("c_d_id"         , new Integer(c_d_id));
    obj.put("c_w_id"         , new Integer(c_w_id));
    obj.put("c_payment_cnt"  , new Integer(c_payment_cnt));
    obj.put("c_delivery_cnt" , new Integer(c_delivery_cnt));
    obj.put("c_since"        , c_since == null ? null : new Long(c_since.getTime()));
    obj.put("c_discount"     , new Float(c_discount));
    obj.put("c_credit_lim"   , new Float(c_credit_lim));
    obj.put("c_balance"      , new Float(c_balance));
    obj.put("c_ytd_payment"  , new Float(c_ytd_payment));
    obj.put("c_credit"       , c_credit);
    obj.put("c_last"         , c_last);
    obj.put("c_first"        , c_first);
    obj.put("c_street_1"     , c_street_1);
    obj.put("c_street_2"     , c_street_2);
    obj.put("c_city"         , c_city);
    obj.put("c_state"        , c_state);
    obj.put("c_zip"          , c_zip);
    obj.put("c_phone"        , c_phone);
    obj.put("c_middle"       , c_middle);
    obj.put("c_data"         , c_data);

    return obj.toString(JSONStyle.MAX_COMPRESS);
  }
  
  public static void main(String[] args) {
      Customer c = new Customer();
      c.c_id = 12345;
      c.c_street_1 = "\"abc";
      String s = c.toJson();
      System.out.println(s);
      Customer c1 = FromJson(s);
      System.out.println(c1);
  }

  public static Customer FromJson(String json) {
    JSONObject o = (JSONObject) JSONValue.parse(json);
    Customer c = new Customer();

    c.c_id           = (Integer) o.get("c_id");
    c.c_d_id         = (Integer) o.get("c_d_id");
    c.c_w_id         = (Integer) o.get("c_w_id");
    c.c_payment_cnt  = (Integer) o.get("c_payment_cnt");
    c.c_delivery_cnt = (Integer) o.get("c_delivery_cnt");
    c.c_since        = o.get("c_since") == null ? null : new Timestamp((Long) o.get("c_since"));
    c.c_discount     = ((Double) o.get("c_discount")).floatValue();
    c.c_credit_lim   = ((Double) o.get("c_credit_lim")).floatValue();
    c.c_balance      = ((Double) o.get("c_balance")).floatValue();
    c.c_ytd_payment  = ((Double) o.get("c_ytd_payment")).floatValue();
    c.c_credit       = (String) o.get("c_credit");
    c.c_last         = (String) o.get("c_last");
    c.c_first        = (String) o.get("c_first");
    c.c_street_1     = (String) o.get("c_street_1");
    c.c_street_2     = (String) o.get("c_street_2");
    c.c_city         = (String) o.get("c_city");
    c.c_state        = (String) o.get("c_state");
    c.c_zip          = (String) o.get("c_zip");
    c.c_phone        = (String) o.get("c_phone");
    c.c_middle       = (String) o.get("c_middle");
    c.c_data         = (String) o.get("c_data");
    
    return c;
  }

	@Override
	public String toString() {
		return ("\n***************** Customer ********************"
				+ "\n*           c_id = "
				+ c_id
				+ "\n*         c_d_id = "
				+ c_d_id
				+ "\n*         c_w_id = "
				+ c_w_id
				+ "\n*     c_discount = "
				+ c_discount
				+ "\n*       c_credit = "
				+ c_credit
				+ "\n*         c_last = "
				+ c_last
				+ "\n*        c_first = "
				+ c_first
				+ "\n*   c_credit_lim = "
				+ c_credit_lim
				+ "\n*      c_balance = "
				+ c_balance
				+ "\n*  c_ytd_payment = "
				+ c_ytd_payment
				+ "\n*  c_payment_cnt = "
				+ c_payment_cnt
				+ "\n* c_delivery_cnt = "
				+ c_delivery_cnt
				+ "\n*     c_street_1 = "
				+ c_street_1
				+ "\n*     c_street_2 = "
				+ c_street_2
				+ "\n*         c_city = "
				+ c_city
				+ "\n*        c_state = "
				+ c_state
				+ "\n*          c_zip = "
				+ c_zip
				+ "\n*        c_phone = "
				+ c_phone
				+ "\n*        c_since = "
				+ c_since
				+ "\n*       c_middle = "
				+ c_middle
				+ "\n*         c_data = " + c_data + "\n**********************************************");
	}

} // end Customer
