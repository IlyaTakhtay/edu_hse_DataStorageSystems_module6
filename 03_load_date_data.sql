/*
 * ==============================================================================
 * PARAMETERS:
 *   :run_day  -> Date of the data to load
 * ==============================================================================
 */

-- ==============================================================================
-- 1. HUBS (Transaction related)
-- ==============================================================================

-- HUB ORDER
INSERT INTO memory.dds.hub_order
SELECT
    to_hex(md5(to_utf8(cast(orderkey as varchar)))) AS hk_order,
    orderkey                                       AS bk_order,
    current_date                                   AS load_date,
    'tpch.tiny'                                    AS record_source
FROM tpch.tiny.orders
WHERE orderdate = :run_day
  AND orderkey NOT IN (SELECT bk_order FROM memory.dds.hub_order);


-- ==============================================================================
-- 2. LINKS
-- ==============================================================================

-- LINK CUSTOMER -> ORDER
INSERT INTO memory.dds.lnk_customer_order
SELECT
    to_hex(md5(to_utf8(json_format(cast(row(custkey, orderkey) as json))))) AS hk_lnk_customer_order,
    to_hex(md5(to_utf8(cast(custkey as varchar))))                          AS hk_customer,
    to_hex(md5(to_utf8(cast(orderkey as varchar))))                         AS hk_order,
    current_date                                                            AS load_date,
    'tpch.tiny'                                                             AS record_source
FROM tpch.tiny.orders
WHERE orderdate = :run_day
  AND to_hex(md5(to_utf8(json_format(cast(row(custkey, orderkey) as json)))))
      NOT IN (SELECT hk_lnk_customer_order FROM memory.dds.lnk_customer_order);


-- LINK ORDER -> PART -> SUPPLIER
INSERT INTO memory.dds.lnk_order_part_supplier
SELECT
    to_hex(md5(to_utf8(json_format(cast(row(
        li.orderkey, li.partkey, li.suppkey, li.linenumber
    ) as json)))))                                  AS hk_lnk_ops,
    to_hex(md5(to_utf8(cast(li.orderkey as varchar)))) AS hk_order,
    to_hex(md5(to_utf8(cast(li.partkey as varchar))))  AS hk_part,
    to_hex(md5(to_utf8(cast(li.suppkey as varchar))))  AS hk_supplier,
    li.linenumber                                      AS line_number,
    current_date                                       AS load_date,
    'tpch.tiny'                                        AS record_source
FROM tpch.tiny.lineitem li
JOIN tpch.tiny.orders o ON o.orderkey = li.orderkey
WHERE o.orderdate = :run_day
  AND to_hex(md5(to_utf8(json_format(cast(row(
          li.orderkey, li.partkey, li.suppkey, li.linenumber
      ) as json)))))
      NOT IN (SELECT hk_lnk_ops FROM memory.dds.lnk_order_part_supplier);


-- ==============================================================================
-- 3. SATELLITES
-- ==============================================================================

-- SAT ORDER
INSERT INTO memory.dds.sat_order
SELECT
    to_hex(md5(to_utf8(cast(orderkey as varchar)))) AS hk_order,
    to_hex(md5(to_utf8(json_format(cast(row(
        orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment
    ) as json)))))                                  AS hashdiff,
    current_date                                    AS load_date,
    'tpch.tiny'                                     AS record_source,
    orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment
FROM tpch.tiny.orders o
WHERE orderdate = :run_day
  AND NOT EXISTS (
      SELECT 1 FROM memory.dds.sat_order s
      WHERE s.hk_order = to_hex(md5(to_utf8(cast(o.orderkey as varchar))))
        AND s.hashdiff = to_hex(md5(to_utf8(json_format(cast(row(
            o.orderstatus, o.totalprice, o.orderdate, o.orderpriority, o.clerk, o.shippriority, o.comment
        ) as json)))))
  );


-- SAT ORDER_PART_SUPPLIER (Lineitem Details)
INSERT INTO memory.dds.sat_order_part_supplier
SELECT
    to_hex(md5(to_utf8(json_format(cast(row(
        li.orderkey, li.partkey, li.suppkey, li.linenumber
    ) as json)))))                                  AS hk_lnk_ops,
    to_hex(md5(to_utf8(json_format(cast(row(
        li.quantity, li.extendedprice, li.discount, li.tax, li.returnflag, li.linestatus,
        li.shipdate, li.commitdate, li.receiptdate, li.shipinstruct, li.shipmode, li.comment
    ) as json)))))                                  AS hashdiff,
    current_date                                    AS load_date,
    'tpch.tiny'                                     AS record_source,
    li.quantity, li.extendedprice, li.discount, li.tax, li.returnflag, li.linestatus,
    li.shipdate, li.commitdate, li.receiptdate, li.shipinstruct, li.shipmode, li.comment
FROM tpch.tiny.lineitem li
JOIN tpch.tiny.orders o ON o.orderkey = li.orderkey
WHERE o.orderdate = :run_day
  AND NOT EXISTS (
      SELECT 1 FROM memory.dds.sat_order_part_supplier s
      WHERE s.hk_lnk_ops = to_hex(md5(to_utf8(json_format(cast(row(
          li.orderkey, li.partkey, li.suppkey, li.linenumber
      ) as json)))))
        AND s.hashdiff   = to_hex(md5(to_utf8(json_format(cast(row(
            li.quantity, li.extendedprice, li.discount, li.tax, li.returnflag, li.linestatus,
            li.shipdate, li.commitdate, li.receiptdate, li.shipinstruct, li.shipmode, li.comment
        ) as json)))))
  );
