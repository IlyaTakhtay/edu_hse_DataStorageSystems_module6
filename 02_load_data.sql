/*
 * ==============================================================================
 * Full load from source (tpch.tiny) into Data Vault (memory.dds)
 *
 * LOGIC:
 *   - Load Date: current_date
 *   - Record Source: 'tpch.tiny'
 * ==============================================================================
 */

-- ==============================================================================
-- 1. HUBS
-- ==============================================================================

INSERT INTO memory.dds.hub_region
SELECT
    to_hex(md5(to_utf8(cast(regionkey as varchar)))) AS hk_region,
    regionkey                                       AS bk_region,
    current_date                                    AS load_date,
    'tpch.tiny'                                     AS record_source
FROM tpch.tiny.region
WHERE regionkey NOT IN (SELECT bk_region FROM memory.dds.hub_region);


INSERT INTO memory.dds.hub_nation
SELECT
    to_hex(md5(to_utf8(cast(nationkey as varchar)))) AS hk_nation,
    nationkey                                       AS bk_nation,
    current_date                                    AS load_date,
    'tpch.tiny'                                     AS record_source
FROM tpch.tiny.nation
WHERE nationkey NOT IN (SELECT bk_nation FROM memory.dds.hub_nation);


INSERT INTO memory.dds.hub_customer
SELECT
    to_hex(md5(to_utf8(cast(custkey as varchar)))) AS hk_customer,
    custkey                                       AS bk_customer,
    current_date                                  AS load_date,
    'tpch.tiny'                                   AS record_source
FROM tpch.tiny.customer
WHERE custkey NOT IN (SELECT bk_customer FROM memory.dds.hub_customer);


INSERT INTO memory.dds.hub_supplier
SELECT
    to_hex(md5(to_utf8(cast(suppkey as varchar)))) AS hk_supplier,
    suppkey                                       AS bk_supplier,
    current_date                                  AS load_date,
    'tpch.tiny'                                   AS record_source
FROM tpch.tiny.supplier
WHERE suppkey NOT IN (SELECT bk_supplier FROM memory.dds.hub_supplier);


INSERT INTO memory.dds.hub_part
SELECT
    to_hex(md5(to_utf8(cast(partkey as varchar)))) AS hk_part,
    partkey                                       AS bk_part,
    current_date                                  AS load_date,
    'tpch.tiny'                                   AS record_source
FROM tpch.tiny.part
WHERE partkey NOT IN (SELECT bk_part FROM memory.dds.hub_part);


INSERT INTO memory.dds.hub_order
SELECT
    to_hex(md5(to_utf8(cast(orderkey as varchar)))) AS hk_order,
    orderkey                                       AS bk_order,
    current_date                                   AS load_date,
    'tpch.tiny'                                    AS record_source
FROM tpch.tiny.orders
WHERE orderkey NOT IN (SELECT bk_order FROM memory.dds.hub_order);


-- ==============================================================================
-- 2. LINKS
-- ==============================================================================

INSERT INTO memory.dds.lnk_nation_region
SELECT
    to_hex(md5(to_utf8(json_format(cast(row(nationkey, regionkey) as json))))) AS hk_lnk_nation_region,
    to_hex(md5(to_utf8(cast(nationkey as varchar))))                           AS hk_nation,
    to_hex(md5(to_utf8(cast(regionkey as varchar))))                           AS hk_region,
    current_date                                                               AS load_date,
    'tpch.tiny'                                                                AS record_source
FROM tpch.tiny.nation
WHERE to_hex(md5(to_utf8(json_format(cast(row(nationkey, regionkey) as json)))))
      NOT IN (SELECT hk_lnk_nation_region FROM memory.dds.lnk_nation_region);


INSERT INTO memory.dds.lnk_customer_order
SELECT
    to_hex(md5(to_utf8(json_format(cast(row(custkey, orderkey) as json))))) AS hk_lnk_customer_order,
    to_hex(md5(to_utf8(cast(custkey as varchar))))                          AS hk_customer,
    to_hex(md5(to_utf8(cast(orderkey as varchar))))                         AS hk_order,
    current_date                                                            AS load_date,
    'tpch.tiny'                                                             AS record_source
FROM tpch.tiny.orders
WHERE to_hex(md5(to_utf8(json_format(cast(row(custkey, orderkey) as json)))))
      NOT IN (SELECT hk_lnk_customer_order FROM memory.dds.lnk_customer_order);


INSERT INTO memory.dds.lnk_order_part_supplier
SELECT
    to_hex(md5(to_utf8(json_format(cast(row(orderkey, partkey, suppkey, linenumber) as json))))) AS hk_lnk_ops,
    to_hex(md5(to_utf8(cast(orderkey as varchar))))                                              AS hk_order,
    to_hex(md5(to_utf8(cast(partkey as varchar))))                                               AS hk_part,
    to_hex(md5(to_utf8(cast(suppkey as varchar))))                                               AS hk_supplier,
    linenumber                                                                                   AS line_number,
    current_date                                                                                 AS load_date,
    'tpch.tiny'                                                                                  AS record_source
FROM tpch.tiny.lineitem
WHERE to_hex(md5(to_utf8(json_format(cast(row(orderkey, partkey, suppkey, linenumber) as json)))))
      NOT IN (SELECT hk_lnk_ops FROM memory.dds.lnk_order_part_supplier);


-- ==============================================================================
-- 3. SATELLITES
-- ==============================================================================

INSERT INTO memory.dds.sat_region
SELECT
    to_hex(md5(to_utf8(cast(regionkey as varchar))))                   AS hk_region,
    to_hex(md5(to_utf8(json_format(cast(row(name, comment) as json))))) AS hashdiff,
    current_date                                                       AS load_date,
    'tpch.tiny'                                                        AS record_source,
    name,
    comment
FROM tpch.tiny.region r
WHERE NOT EXISTS (
    SELECT 1 FROM memory.dds.sat_region s
    WHERE s.hk_region = to_hex(md5(to_utf8(cast(r.regionkey as varchar))))
      AND s.hashdiff  = to_hex(md5(to_utf8(json_format(cast(row(r.name, r.comment) as json)))))
);


INSERT INTO memory.dds.sat_nation
SELECT
    to_hex(md5(to_utf8(cast(nationkey as varchar))))                   AS hk_nation,
    to_hex(md5(to_utf8(json_format(cast(row(name, comment) as json))))) AS hashdiff,
    current_date                                                       AS load_date,
    'tpch.tiny'                                                        AS record_source,
    name,
    comment
FROM tpch.tiny.nation n
WHERE NOT EXISTS (
    SELECT 1 FROM memory.dds.sat_nation s
    WHERE s.hk_nation = to_hex(md5(to_utf8(cast(n.nationkey as varchar))))
      AND s.hashdiff  = to_hex(md5(to_utf8(json_format(cast(row(n.name, n.comment) as json)))))
);


INSERT INTO memory.dds.sat_customer
SELECT
    to_hex(md5(to_utf8(cast(custkey as varchar)))) AS hk_customer,
    to_hex(md5(to_utf8(json_format(cast(row(
        name, address, phone, acctbal, mktsegment, comment, nationkey
    ) as json)))))                                 AS hashdiff,
    current_date                                   AS load_date,
    'tpch.tiny'                                    AS record_source,
    name, address, phone, acctbal, mktsegment, comment, nationkey
FROM tpch.tiny.customer c
WHERE NOT EXISTS (
    SELECT 1 FROM memory.dds.sat_customer s
    WHERE s.hk_customer = to_hex(md5(to_utf8(cast(c.custkey as varchar))))
      AND s.hashdiff    = to_hex(md5(to_utf8(json_format(cast(row(
          c.name, c.address, c.phone, c.acctbal, c.mktsegment, c.comment, c.nationkey
      ) as json)))))
);


INSERT INTO memory.dds.sat_supplier
SELECT
    to_hex(md5(to_utf8(cast(suppkey as varchar)))) AS hk_supplier,
    to_hex(md5(to_utf8(json_format(cast(row(
        name, address, phone, acctbal, comment, nationkey
    ) as json)))))                                 AS hashdiff,
    current_date                                   AS load_date,
    'tpch.tiny'                                    AS record_source,
    name, address, phone, acctbal, comment, nationkey
FROM tpch.tiny.supplier s
WHERE NOT EXISTS (
    SELECT 1 FROM memory.dds.sat_supplier t
    WHERE t.hk_supplier = to_hex(md5(to_utf8(cast(s.suppkey as varchar))))
      AND t.hashdiff    = to_hex(md5(to_utf8(json_format(cast(row(
          s.name, s.address, s.phone, s.acctbal, s.comment, s.nationkey
      ) as json)))))
);


INSERT INTO memory.dds.sat_part
SELECT
    to_hex(md5(to_utf8(cast(partkey as varchar)))) AS hk_part,
    to_hex(md5(to_utf8(json_format(cast(row(
        name, mfgr, brand, type, size, container, retailprice, comment
    ) as json)))))                                 AS hashdiff,
    current_date                                   AS load_date,
    'tpch.tiny'                                    AS record_source,
    name, mfgr, brand, type, size, container, retailprice, comment
FROM tpch.tiny.part p
WHERE NOT EXISTS (
    SELECT 1 FROM memory.dds.sat_part s
    WHERE s.hk_part  = to_hex(md5(to_utf8(cast(p.partkey as varchar))))
      AND s.hashdiff = to_hex(md5(to_utf8(json_format(cast(row(
          p.name, p.mfgr, p.brand, p.type, p.size, p.container, p.retailprice, p.comment
      ) as json)))))
);


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
WHERE NOT EXISTS (
    SELECT 1 FROM memory.dds.sat_order s
    WHERE s.hk_order = to_hex(md5(to_utf8(cast(o.orderkey as varchar))))
      AND s.hashdiff = to_hex(md5(to_utf8(json_format(cast(row(
          o.orderstatus, o.totalprice, o.orderdate, o.orderpriority, o.clerk, o.shippriority, o.comment
      ) as json)))))
);


INSERT INTO memory.dds.sat_order_part_supplier
SELECT
    to_hex(md5(to_utf8(json_format(cast(row(
        orderkey, partkey, suppkey, linenumber
    ) as json)))))                                  AS hk_lnk_ops,
    to_hex(md5(to_utf8(json_format(cast(row(
        quantity, extendedprice, discount, tax, returnflag, linestatus,
        shipdate, commitdate, receiptdate, shipinstruct, shipmode, comment
    ) as json)))))                                  AS hashdiff,
    current_date                                    AS load_date,
    'tpch.tiny'                                     AS record_source,
    quantity, extendedprice, discount, tax, returnflag, linestatus,
    shipdate, commitdate, receiptdate, shipinstruct, shipmode, comment
FROM tpch.tiny.lineitem li
WHERE NOT EXISTS (
    SELECT 1 FROM memory.dds.sat_order_part_supplier s
    WHERE s.hk_lnk_ops = to_hex(md5(to_utf8(json_format(cast(row(
        li.orderkey, li.partkey, li.suppkey, li.linenumber
    ) as json)))))
      AND s.hashdiff   = to_hex(md5(to_utf8(json_format(cast(row(
          li.quantity, li.extendedprice, li.discount, li.tax, li.returnflag, li.linestatus,
          li.shipdate, li.commitdate, li.receiptdate, li.shipinstruct, li.shipmode, li.comment
      ) as json)))))
);
