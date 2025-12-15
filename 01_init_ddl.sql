-- =========================================================
-- DDS schema (Data Vault)
-- Source: tpch.tiny
-- =========================================================

CREATE SCHEMA IF NOT EXISTS memory.dds;

-- =========================================================
-- 1) HUBS
-- =========================================================

CREATE TABLE IF NOT EXISTS memory.dds.hub_order (
    hk_order      VARCHAR,
    bk_order      BIGINT,
    load_date     DATE,
    record_source VARCHAR
);

CREATE TABLE IF NOT EXISTS memory.dds.hub_part (
    hk_part       VARCHAR,
    bk_part       BIGINT,
    load_date     DATE,
    record_source VARCHAR
);

CREATE TABLE IF NOT EXISTS memory.dds.hub_supplier (
    hk_supplier   VARCHAR,
    bk_supplier   BIGINT,
    load_date     DATE,
    record_source VARCHAR
);

CREATE TABLE IF NOT EXISTS memory.dds.hub_customer (
    hk_customer   VARCHAR,
    bk_customer   BIGINT,
    load_date     DATE,
    record_source VARCHAR
);

CREATE TABLE IF NOT EXISTS memory.dds.hub_nation (
    hk_nation     VARCHAR,
    bk_nation     BIGINT,
    load_date     DATE,
    record_source VARCHAR
);

CREATE TABLE IF NOT EXISTS memory.dds.hub_region (
    hk_region     VARCHAR,
    bk_region     BIGINT,
    load_date     DATE,
    record_source VARCHAR
);

-- =========================================================
-- 2) LINKS
-- =========================================================

-- Customer ↔ Order
CREATE TABLE IF NOT EXISTS memory.dds.lnk_customer_order (
    hk_lnk_customer_order VARCHAR,
    hk_customer           VARCHAR,
    hk_order              VARCHAR,
    load_date             DATE,
    record_source         VARCHAR
);

-- Nation ↔ Region
CREATE TABLE IF NOT EXISTS memory.dds.lnk_nation_region (
    hk_lnk_nation_region VARCHAR,
    hk_nation            VARCHAR,
    hk_region            VARCHAR,
    load_date            DATE,
    record_source        VARCHAR
);

-- Order ↔ Part ↔ Supplier
CREATE TABLE IF NOT EXISTS memory.dds.lnk_order_part_supplier (
    hk_lnk_ops    VARCHAR,
    hk_order      VARCHAR,
    hk_part       VARCHAR,
    hk_supplier   VARCHAR,
    line_number   INTEGER,
    load_date     DATE,
    record_source VARCHAR
);

-- =========================================================
-- 3) SATELLITES ON HUBS
-- =========================================================

CREATE TABLE IF NOT EXISTS memory.dds.sat_order (
    hk_order         VARCHAR,
    hashdiff         VARCHAR,
    load_date        DATE,
    record_source    VARCHAR,
    o_orderstatus    VARCHAR,
    o_totalprice     DOUBLE,
    o_orderdate      DATE,
    o_orderpriority  VARCHAR,
    o_clerk          VARCHAR,
    o_shippriority   INTEGER,
    o_comment        VARCHAR
);

CREATE TABLE IF NOT EXISTS memory.dds.sat_part (
    hk_part       VARCHAR,
    hashdiff      VARCHAR,
    load_date     DATE,
    record_source VARCHAR,
    p_name        VARCHAR,
    p_mfgr        VARCHAR,
    p_brand       VARCHAR,
    p_type        VARCHAR,
    p_size        INTEGER,
    p_container   VARCHAR,
    p_retailprice DOUBLE,
    p_comment     VARCHAR
);

CREATE TABLE IF NOT EXISTS memory.dds.sat_supplier (
    hk_supplier   VARCHAR,
    hashdiff      VARCHAR,
    load_date     DATE,
    record_source VARCHAR,
    s_name        VARCHAR,
    s_address     VARCHAR,
    s_phone       VARCHAR,
    s_acctbal     DOUBLE,
    s_comment     VARCHAR,
    n_nationkey   BIGINT
);

CREATE TABLE IF NOT EXISTS memory.dds.sat_customer (
    hk_customer   VARCHAR,
    hashdiff      VARCHAR,
    load_date     DATE,
    record_source VARCHAR,
    c_name        VARCHAR,
    c_address     VARCHAR,
    c_phone       VARCHAR,
    c_acctbal     DOUBLE,
    c_mktsegment  VARCHAR,
    c_comment     VARCHAR,
    n_nationkey   BIGINT
);

CREATE TABLE IF NOT EXISTS memory.dds.sat_nation (
    hk_nation     VARCHAR,
    hashdiff      VARCHAR,
    load_date     DATE,
    record_source VARCHAR,
    n_name        VARCHAR,
    n_comment     VARCHAR
);

CREATE TABLE IF NOT EXISTS memory.dds.sat_region (
    hk_region     VARCHAR,
    hashdiff      VARCHAR,
    load_date     DATE,
    record_source VARCHAR,
    r_name        VARCHAR,
    r_comment     VARCHAR
);

-- =========================================================
-- 4) SATELLITE ON LINK (lineitem attributes)
-- Attributes belong to the relationship (order, part, supplier, line_number)
-- =========================================================

CREATE TABLE IF NOT EXISTS memory.dds.sat_order_part_supplier (
    hk_lnk_ops      VARCHAR,
    hashdiff        VARCHAR,
    load_date       DATE,
    record_source   VARCHAR,
    l_quantity      DOUBLE,
    l_extendedprice DOUBLE,
    l_discount      DOUBLE,
    l_tax           DOUBLE,
    l_returnflag    VARCHAR,
    l_linestatus    VARCHAR,
    l_shipdate      DATE,
    l_commitdate    DATE,
    l_receiptdate   DATE,
    l_shipinstruct  VARCHAR,
    l_shipmode      VARCHAR,
    l_comment       VARCHAR
);
