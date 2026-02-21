# -*- coding: utf-8 -*-
# @author Janani P.
# PySpark / AWS Glue version of: Prepare BA ABP and BA ABP Unique delta repositories for Offline process.

import os
import sys
import json
import traceback
import logging
from datetime import datetime

import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --------------------------------------------------------------------------------------
# Spark + Logging
# --------------------------------------------------------------------------------------
spark = SparkSession.builder.appName("BA_ABP_Unique_Repo_PySpark").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

logger = logging.getLogger("BA_ABP_UNIQUE")
logger.setLevel(logging.INFO)
if not logger.handlers:
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(h)


def log_count(df, step_name: str):
    cnt = df.count()
    logger.info(f"[COUNT] {step_name}: {cnt}")
    return df


def now_yyyymmddhhmmss_col():
    return F.date_format(F.current_timestamp(), "yyyyMMddHHmmss")


# --------------------------------------------------------------------------------------
# Args / Env (Glue: prefer sys.argv; fallback to env vars)
# --------------------------------------------------------------------------------------
def get_arg_or_env(idx: int, env_key: str, default=None):
    if len(sys.argv) > idx and sys.argv[idx]:
        return sys.argv[idx]
    return os.environ.get(env_key, default)


ba_abp_inp_path = get_arg_or_env(1, "ba_abp_inp")
ba_abp_ins_path = get_arg_or_env(2, "ba_abp_ins_frnd_path")
ba_abp_unique_exist_path = get_arg_or_env(3, "ba_abp_unique_delta_inp")
ba_abp_exist_path = get_arg_or_env(4, "ba_abp_stg_path")  # not used in lambda code shown
tpo_out_path = get_arg_or_env(5, "tpo_part2_out_path")
ba_abp_unique_delta_loc = get_arg_or_env(6, "ba_abp_unique_delta_output_loc")
ba_abp_unique_delta_exist_loc = get_arg_or_env(7, "ba_abp_unique_delta_exist_output_loc")
ba_abp_with_addr_priority_op_path = get_arg_or_env(8, "ba_abp_with_addr_priority_op_path")
ba_abp_repo_op_path = get_arg_or_env(9, "ba_abp_repo_op_path")
force_create_and_level_loc = get_arg_or_env(10, "force_create_and_level_loc")
ngu_repo_stg_loc = get_arg_or_env(11, "ngu_repo_stg_loc")
ba_abp_parent_child_loc = get_arg_or_env(12, "ba_abp_parent_child_loc")

env = os.environ.get("env", "")
sns_arn = os.environ.get("sns_arn", "")
mail_subject = f"Error in BA ABP Unique Repo process - {env}!!!"

# SSM parameter *names* are stored in env vars in lambda.
ssm_ba_abp_col_list_param_name = os.environ.get("ba_abp_col_list")  # -> actual ssm parameter name
ssm_dpa_col_list_param_name = os.environ.get("baabp_dpa_col_list")
ssm_oaa_col_list_param_name = os.environ.get("ba_abp_oaa_col_list")
ssm_ba_abp_unique_schema_param_name = os.environ.get("ba_abp_unique_schema")
ssm_ba_abp_unique_agg_param1_param_name = os.environ.get("ba_abp_unique_agg_param1")
ssm_ba_abp_unique_agg_param2_param_name = os.environ.get("ba_abp_unique_agg_param2")

sns_client = boto3.client("sns", region_name="eu-west-2")
ssm_client = boto3.client("ssm")


# --------------------------------------------------------------------------------------
# Helpers (string normalize, rounding/formatting like pandas apply)
# --------------------------------------------------------------------------------------
def col_trim_upper(c):
    return F.upper(F.trim(F.coalesce(F.col(c), F.lit(""))))


def col_trim(c):
    return F.trim(F.coalesce(F.col(c), F.lit("")))


def to_num_round_format_str(c, decimals=1):
    """
    Lambda:
      pd.to_numeric(errors='coerce').round(1).apply(lambda x: str(int(x)) if is_integer else f'{x:.1f}' else '')
    Spark equivalent returns STRING.
    """
    raw = F.col(c)
    # treat '' as null
    cleaned = F.when(F.trim(F.coalesce(raw.cast("string"), F.lit(""))) == "", F.lit(None)).otherwise(raw.cast("double"))
    r = F.round(cleaned, decimals)

    # integer check: r == floor(r)
    is_int = (r.isNotNull()) & (r == F.floor(r))
    as_int_str = F.format_string("%d", r.cast("int"))
    as_1dp_str = F.format_string("%.1f", r.cast("double"))

    return F.when(r.isNull(), F.lit("")).when(is_int, as_int_str).otherwise(as_1dp_str)


def strip_dot_zero_str(c):
    # remove trailing ".0" from string columns (like pandas .str.replace('.0',''))
    return F.regexp_replace(F.coalesce(F.col(c).cast("string"), F.lit("")), r"\.0$", "")


# --------------------------------------------------------------------------------------
# Lambda: initcaps
# Spark: initcap covers most; lambda only capitalizes tokens beginning with alpha; good enough for parity here.
# --------------------------------------------------------------------------------------
def initcaps_expr(c):
    return F.initcap(F.coalesce(F.col(c).cast("string"), F.lit("")))


# --------------------------------------------------------------------------------------
# Lambda: assign_address_priority
# --------------------------------------------------------------------------------------
def assign_address_priority(df):
    df = df.withColumn(
        "language_priority",
        F.when(
            (col_trim_upper("language") == "ENG")
            | ((col_trim("language") == F.lit("")) & (col_trim_upper("type").isin("PAF", "OAA", "LPI-APPROVED", "LPI-PROVISIONAL"))),
            F.lit("1"),
        ).otherwise(F.lit("2")),
    )

    df = df.withColumn(
        "address_priority_new",
        F.when((col_trim("address_priority") != "") & (F.length(col_trim("address_priority")) == 1), col_trim("address_priority"))
        .when((col_trim("address_priority") == "") & (col_trim_upper("type") == "LPI-ALTERNATE"), F.lit("5"))
        .when((col_trim("address_priority") == "") & (col_trim_upper("type") == "DPA"), F.lit("6"))
        .when((col_trim("address_priority") == "") & (col_trim_upper("type") == "PAF"), F.lit("7"))
        .otherwise(F.lit("8")),
    )
    return df


# --------------------------------------------------------------------------------------
# Lambda: get_child_nad_and_uprn_list
# --------------------------------------------------------------------------------------
def get_child_nad_and_uprn_list(df):
    child = spark.read.parquet(ba_abp_parent_child_loc)
    # add suffix _child like lambda
    for c in child.columns:
        child = child.withColumnRenamed(c, f"{c}_child")

    joined = df.join(child, df["nad_key"] == child["nad_key_child"], "left") \
               .withColumnRenamed("childnadkeyuprn_child", "childnadkeyuprn") \
               .dropDuplicates() \
               .fillna("")
    return joined


# --------------------------------------------------------------------------------------
# Lambda: mask_for_css_subprem_and_css_prem (vectorized as spark expression)
# return (premises_name != '') and ((thoroughfare != '' and thoroughfare in premises_name) or (dependent_thoroughfare != '' and dependent_thoroughfare in premises_name))
# --------------------------------------------------------------------------------------
def mask_for_css_subprem_and_css_prem_expr():
    premises_name = col_trim("lpi_summary_premisesname")
    thoroughfare = col_trim("paf_css_thoroughfare_name")
    dep_thoroughfare = col_trim("paf_or_friendly_dependentthoroughfarename")

    cond1 = (premises_name != "") & (thoroughfare != "") & (F.instr(premises_name, thoroughfare) > 0)
    cond2 = (premises_name != "") & (dep_thoroughfare != "") & (F.instr(premises_name, dep_thoroughfare) > 0)
    return cond1 | cond2


# --------------------------------------------------------------------------------------
# Lambda: handle_promote_or_demote (full parity for shown logic + OR friendly selection + OAA/DPA columns + flags + final select)
# --------------------------------------------------------------------------------------
def handle_promote_or_demote(df, col_list, oaa_col_list, dpa_col_list):
    # Identify column lists exactly like lambda
    or_friendly_cols = [c for c in df.columns if c.startswith("or_friendly_")]
    lpi_orfriendly_cols = [c for c in df.columns if c.startswith("lpi_or_friendly_")]

    lpi_orfriendly_col_dpa_list = [c for c in lpi_orfriendly_cols if c != "lpi_or_friendly_county"]
    lpi_orfriendly_col_oaa_list = [c for c in lpi_orfriendly_cols if c not in ["lpi_or_friendly_county", "lpi_or_friendly_dependentthoroughfarename", "lpi_or_friendly_doubledependentlocality"]]

    # Clear listed columns for non-top-ranked
    for c in col_list:
        if c in df.columns:
            df = df.withColumn(c, F.when(F.col("dedup_rnk") == 1, F.col(c)).otherwise(F.lit("")))

    # OR Friendly column selection based on Promote/Demote
    for c in or_friendly_cols:
        # col_source = 'lpi_' + col_name if col_name not in ['or_friendly_postincode', 'or_friendly_postoutcode', 'or_friendly_pobox'] else col_name
        if c not in ["or_friendly_postincode", "or_friendly_postoutcode", "or_friendly_pobox"]:
            col_source = f"lpi_{c}"
        else:
            col_source = c

        # if col_name == 'or_friendly_pobox' : always blank in places lambda uses col_source
        col_source_value = F.lit("") if c == "or_friendly_pobox" else F.coalesce(F.col(col_source), F.lit(""))

        df = df.withColumn(
            c,
            F.when(
                (F.col("req_type") == "PROMOTE")
                & (F.col("promoted_type") == F.col("type"))
                & (F.col("promoted_type").isin("OAA", "DPA")),
                col_source_value,
            )
            .when((F.col("req_type") == "PROMOTE") & (F.col("promoted_type") == F.col("type")), F.coalesce(F.col(c), F.lit("")))
            .when((F.col("req_type") == "PROMOTE") & (F.col("promoted_type") == "LPIA") & (F.col("lpi_key") == F.col("lpia_key")), col_source_value)
            .when((F.col("req_type") == "PROMOTE") & (F.col("promoted_type").isin("LPI", "NGD")) & (F.col("dedup_rnk") == 1), col_source_value)
            .when(
                (
                    ((F.col("req_type") == "PROMOTE") & (F.col("promoted_type") == "PAF_retain"))
                    | (F.col("req_type").isin("DEMOTE", "UPDATE"))
                    | (F.col("req_type") == "")
                )
                & (F.col("dedup_rnk") == 1),
                F.coalesce(F.col(c), F.lit("")),
            )
            .otherwise(F.lit("")),
        )

    # or_friendly_level
    df = df.withColumn(
        "or_friendly_level",
        F.when((F.col("req_type") == "PROMOTE") & (F.col("promoted_type") == F.col("type")), F.coalesce(F.col("level"), F.lit("")))
        .when((F.col("req_type") == "PROMOTE") & (F.col("promoted_type") == "LPIA") & (F.col("lpi_key") == F.col("lpia_key")), F.coalesce(F.col("level"), F.lit("")))
        .when((F.col("req_type") == "PROMOTE") & (F.col("promoted_type") == "OAA"), F.coalesce(F.col("level"), F.lit("")))
        .when(
            ((F.col("req_type") == "PROMOTE") & (F.col("promoted_type").isin("LPI", "NGD", "PAF_retain")))
            | ((F.col("req_type").isin("DEMOTE", "UPDATE")) | (F.col("req_type") == "")) & (F.col("dedup_rnk") == 1),
            F.coalesce(F.col("level"), F.lit("")),
        )
        .otherwise(F.lit("")),
    )

    # LPIA demote -> lpia_key must be empty
    df = df.withColumn("lpia_key", F.when((F.col("req_type") == "DEMOTE") & (F.col("promoted_type") == "LPIA"), F.lit("")).otherwise(col_trim("lpia_key")))

    # OAA Promote / Demote: update non-address columns
    oaa_promote_non_addr_cols = ["easting", "northing", "latitude", "longitude", "site_identifier", "site_classification_desc", "street_record_type_code", "rpc"]
    for c in oaa_promote_non_addr_cols:
        if c in df.columns:
            df = df.withColumn(
                c,
                F.when(
                    (F.col("req_type") == "PROMOTE") & (F.col("promoted_type") == "OAA"),
                    F.when(F.col("type") == "OAA", F.coalesce(F.col(c), F.lit(""))).otherwise(F.lit("")),
                ).otherwise(F.coalesce(F.col(c), F.lit(""))),
            )

    for c in oaa_promote_non_addr_cols:
        if c in df.columns:
            df = df.withColumn(
                c,
                F.when(
                    (F.col("req_type") == "DEMOTE") & (F.col("promoted_type") == "OAA"),
                    F.when(F.col("type") != "OAA", F.coalesce(F.col(c), F.lit(""))).otherwise(F.lit("")),
                ).otherwise(F.coalesce(F.col(c), F.lit(""))),
            )

    # OAA columns creation from lpi_or_friendly list
    oaa_cols_filtered = [c for c in oaa_col_list if c != "oaa_subpremisesnumber"]
    for idx in range(min(len(oaa_cols_filtered), len(lpi_orfriendly_col_oaa_list))):
        oaa_c = oaa_cols_filtered[idx]
        src_c = lpi_orfriendly_col_oaa_list[idx]
        if oaa_c in df.columns and src_c in df.columns:
            df = df.withColumn(oaa_c, F.when(F.col("type") == "OAA", F.coalesce(F.col(src_c), F.lit(""))).otherwise(F.lit("")))
        elif src_c in df.columns:
            df = df.withColumn(oaa_c, F.when(F.col("type") == "OAA", F.coalesce(F.col(src_c), F.lit(""))).otherwise(F.lit("")))

    df = df.withColumn("oaa_subpremisesnumber", F.lit(""))
    df = df.withColumn("oaa_easting", F.when(F.col("type") == "OAA", F.coalesce(F.col("easting"), F.lit(""))).otherwise(F.lit("")))
    df = df.withColumn("oaa_northing", F.when(F.col("type") == "OAA", F.coalesce(F.col("northing"), F.lit(""))).otherwise(F.lit("")))
    df = df.withColumn("oaa_latitude", F.when(F.col("type") == "OAA", F.coalesce(F.col("latitude"), F.lit(""))).otherwise(F.lit("")))
    df = df.withColumn("oaa_longitude", F.when(F.col("type") == "OAA", F.coalesce(F.col("longitude"), F.lit(""))).otherwise(F.lit("")))
    df = df.withColumn("oaa_site_code", F.when(F.col("type") == "OAA", F.coalesce(F.col("site_identifier"), F.lit(""))).otherwise(F.lit("")))
    df = df.withColumn("oaa_site_description", F.when(F.col("type") == "OAA", F.coalesce(F.col("site_classification_desc"), F.lit(""))).otherwise(F.lit("")))
    df = df.withColumn("oaa_level", F.when(F.col("type") == "OAA", F.coalesce(F.col("level"), F.lit(""))).otherwise(F.lit("")))
    df = df.withColumn("oaa_streettypecode", F.when(F.col("type") == "OAA", F.coalesce(F.col("street_record_type_code"), F.lit(""))).otherwise(F.lit("")))
    df = df.withColumn("oaa_rpc", F.when(F.col("type") == "OAA", F.coalesce(F.col("rpc"), F.lit(""))).otherwise(F.lit("")))

    # DPA columns creation from lpi_or_friendly list
    for idx in range(min(len(dpa_col_list), len(lpi_orfriendly_col_dpa_list))):
        dpa_c = dpa_col_list[idx]
        src_c = lpi_orfriendly_col_dpa_list[idx]
        if src_c in df.columns:
            df = df.withColumn(dpa_c, F.when(F.col("type") == "DPA", F.coalesce(F.col(src_c), F.lit(""))).otherwise(F.lit("")))

    df = df.withColumn("dpa_po_box", F.lit(""))
    df = df.withColumn("dpa_alias_premises_name", F.lit(""))

    # Flag columns
    df = df.withColumn("lpipresentflagcnt", F.when(F.col("type").startswith("LPI"), F.lit(1)).otherwise(F.lit(0)))
    df = df.withColumn("ngdpresentflag", F.when(F.col("type").startswith("LPI"), F.lit("TRUE")).otherwise(F.lit("")))
    df = df.withColumn("dpapresentflag", F.when(F.col("type") == "DPA", F.lit("TRUE")).otherwise(F.lit("")))
    df = df.withColumn("oaapresentflag", F.when(F.col("type") == "OAA", F.lit("TRUE")).otherwise(F.lit("")))
    df = df.withColumn("lpi_level", F.when(F.col("dedup_rnk") == 1, F.coalesce(F.col("level"), F.lit(""))).otherwise(F.lit("")))
    df = df.withColumn("ngd_level", F.when(F.col("dedup_rnk") == 1, F.coalesce(F.col("level"), F.lit(""))).otherwise(F.lit("")))
    df = df.withColumn("dpa_departmentname", F.when(F.col("type") == "DPA", F.coalesce(F.col("department_name"), F.lit(""))).otherwise(F.lit("")))

    # Final column ordering must match lambda list
    final_cols = [
        'nad_key', 'parent_nad_key', 'nad_start_date', 'modify_timestamp', 'address_quality', 'org_name', 'po_box', 'sub_premise', 'premise_name',
        'thoroughfare_number', 'depend_thouroughfare', 'thoroughfare', 'double_depend_locality', 'locality', 'town', 'county', 'postal_outcode',
        'postal_incode', 'delivery_point', 'fail_level', 'best_address_line_1', 'best_address_line_2', 'best_address_line_3', 'best_address_line_4',
        'best_postcode', 'country_name', 'address_status', 'address_structure', 'easting', 'northing', 'district_code', 'db_id_mismatch_flag',
        'exchange_group_code', 'udprn', 'parent_uprn', 'uprn', 'latitude', 'longitude', 'site_identifier', 'source', 'postal_flag',
        'multi_classification_flag', 'site_classification_desc', 'copper', 'p2pfibre', 'fttp_greenfield', 'fttp_brownfield',
        'lpi_org_name', 'lpi_sao_start_number', 'lpi_sao_start_suffix', 'lpi_sao_end_number', 'lpi_sao_end_suffix', 'lpi_sao_text',
        'lpi_pao_start_number', 'lpi_pao_start_suffix', 'lpi_pao_end_number', 'lpi_pao_end_suffix', 'lpi_pao_text',
        'lpi_street_descriptor', 'lpi_locality_name', 'lpi_town', 'lpi_postcode',
        'css_sub_premises', 'css_premises_name', 'css_thoroughfare_number', 'css_thoroughfare_name', 'css_locality', 'css_post_town',
        'css_county', 'css_postcode',
        'or_friendly_organisation', 'or_friendly_buildingname', 'or_friendly_subpremise', 'or_friendly_thoroughfarenumber',
        'or_friendly_thoroughfarename', 'or_friendly_locality', 'or_friendly_posttown', 'or_friendly_postcode', 'or_friendly_postincode',
        'or_friendly_postoutcode', 'or_friendly_county', 'or_friendly_dependentthoroughfarename', 'or_friendly_doubledependentlocality',
        'or_friendly_pobox',
        '1141code', 'street_record_type_code', 'logical_status', 'rpc', 'w3w',
        'lpi_summary_organisation', 'lpi_summary_subpremisesname', 'lpi_summary_subpremisesnumber', 'lpi_summary_premisesname',
        'lpi_summary_premisesnumber', 'lpi_summary_thoroughfare', 'lpi_summary_locality', 'lpi_summary_town', 'lpi_summary_postcode',
        'bronze_key', 'usrn',
        'paf_org_name', 'paf_po_box', 'paf_sub_premises', 'paf_premise_name', 'paf_thoroughfare_number', 'paf_depend_thouroughfare',
        'paf_thoroughfare', 'paf_double_depend_locality', 'paf_locality', 'paf_town', 'paf_county', 'paf_postcode',
        'lpi_or_friendly_organisation', 'lpi_or_friendly_subpremise', 'lpi_or_friendly_buildingname', 'lpi_or_friendly_thoroughfarenumber',
        'lpi_or_friendly_thoroughfarename', 'lpi_or_friendly_locality', 'lpi_or_friendly_posttown', 'lpi_or_friendly_postcode',
        'lpi_or_friendly_county', 'lpi_or_friendly_dependentthoroughfarename', 'lpi_or_friendly_doubledependentlocality',
        'inherited_udprn',
        'paf_or_friendly_organisation', 'paf_or_friendly_subpremise', 'paf_or_friendly_buildingname', 'paf_or_friendly_thoroughfarenumber',
        'paf_or_friendly_thoroughfarename', 'paf_or_friendly_locality', 'paf_or_friendly_posttown', 'paf_or_friendly_postcode',
        'paf_or_friendly_county', 'paf_or_friendly_dependentthoroughfarename', 'paf_or_friendly_doubledependentlocality',
        'paf_css_sub_premises', 'paf_css_premises_name', 'paf_css_thoroughfare_number', 'paf_css_thoroughfare_name', 'paf_css_locality',
        'paf_css_post_town', 'paf_css_county', 'paf_css_postcode',
        'nad_end_date', 'child_nad_key_count', 'type', 'abp_child_uprn_count', 'toid', 'org_concatenated', 'lpi_key', 'postal_address_code',
        'official_flag_code', 'start_date', 'end_date', 'last_update_date', 'entry_date', 'level', 'area_name', 'usrn_match_indicator',
        'street_classification', 'blpu_state_code', 'address_priority', 'language', 'historic_reason', 'location', 'active_asset',
        'proposed_to_be_historic', 'burst_start_date', 'burst_end_date',
        'dpa_org_name', 'dpa_po_box', 'dpa_sub_premises', 'dpa_premises_name', 'dpa_alias_premises_name', 'dpa_thoroughfare_number',
        'dpa_dependent_thoroughfare', 'dpa_thoroughfare_name', 'dpa_double_dependent_locality', 'dpa_locality', 'dpa_town', 'dpa_postcode',
        'oaa_organisation', 'oaa_subpremisesnumber', 'oaa_subpremisesname', 'oaa_premisesname', 'oaa_premisesnumber', 'oaa_thoroughfare',
        'oaa_locality', 'oaa_town', 'oaa_postcode', 'oaa_easting', 'oaa_northing', 'oaa_latitude', 'oaa_longitude', 'oaa_site_code',
        'oaa_site_description', 'oaa_level', 'oaa_streettypecode', 'oaa_rpc',
        'req_type', 'promoted_type', 'lpia_key', 'lpipresentflagcnt', 'ngdpresentflag', 'dpapresentflag', 'oaapresentflag', 'lpi_level',
        'ngd_level', 'or_friendly_level', 'dpa_departmentname',
        'ins_sub_premises', 'ins_premises_name', 'ins_thoroughfare_number', 'ins_thoroughfare_name', 'ins_locality', 'ins_post_town',
        'ins_postcode', 'ins_level', 'ins_room',
        'childnadkeyuprn', 'force_create_flag'
    ]

    # Ensure missing columns exist as empty strings (Spark will error if select missing)
    for c in final_cols:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(""))

    return df.select(*final_cols)


# --------------------------------------------------------------------------------------
# Lambda: fetch_unique_record (groupBy nad_key)
# --------------------------------------------------------------------------------------
def fetch_unique_record(df, agg_param_map):
    """
    agg_param_map like: {"col1":"max","col2":"sum", ...}
    """
    agg_exprs = []
    for c, fn in agg_param_map.items():
        if fn == "max":
            agg_exprs.append(F.max(F.col(c)).alias(c))
        elif fn == "sum":
            # in lambda sum on flags/ints
            agg_exprs.append(F.sum(F.col(c).cast("long")).alias(c))
        else:
            raise ValueError(f"Unsupported agg fn: {fn} for column {c}")

    out = df.groupBy("nad_key").agg(*agg_exprs)

    out = out.withColumn("promoted_type", F.when(F.col("promoted_type") == "PAF_retain", F.lit("PAF")).otherwise(F.col("promoted_type")))
    out = out.withColumn("lpipresentflag", F.when(F.col("lpipresentflagcnt") >= 1, F.lit("TRUE")).otherwise(F.lit("FALSE")))
    out = out.withColumn("lpialternatepresentflag", F.when(F.col("lpipresentflagcnt") > 1, F.lit("TRUE")).otherwise(F.lit("FALSE")))
    out = out.withColumn("ngdpresentflag", F.when(F.col("ngdpresentflag") == "", F.lit("FALSE")).otherwise(F.lit("TRUE")))
    out = out.withColumn("dpapresentflag", F.when(F.col("dpapresentflag") == "", F.lit("FALSE")).otherwise(F.lit("TRUE")))
    out = out.withColumn("oaapresentflag", F.when(F.col("oaapresentflag") == "", F.lit("FALSE")).otherwise(F.lit("TRUE")))
    return out


# --------------------------------------------------------------------------------------
# Lambda: calculate_timestamps
# --------------------------------------------------------------------------------------
def calculate_timestamps(df):
    ts_now = now_yyyymmddhhmmss_col()

    addr_changed = (
        (F.col("nad_key_baabp").isNotNull())
        & (
            (col_trim_upper("or_friendly_organisation") != col_trim_upper("or_friendly_organisation_baabp"))
            | (col_trim_upper("or_friendly_pobox") != col_trim_upper("or_friendly_pobox_baabp"))
            | (col_trim_upper("or_friendly_subpremise") != col_trim_upper("or_friendly_subpremise_baabp"))
            | (col_trim_upper("or_friendly_buildingname") != col_trim_upper("or_friendly_buildingname_baabp"))
            | (col_trim_upper("or_friendly_thoroughfarenumber") != col_trim_upper("or_friendly_thoroughfarenumber_baabp"))
            | (col_trim_upper("or_friendly_dependentthoroughfarename") != col_trim_upper("or_friendly_dependentthoroughfarename_baabp"))
            | (col_trim_upper("or_friendly_thoroughfarename") != col_trim_upper("or_friendly_thoroughfarename_baabp"))
            | (col_trim_upper("or_friendly_doubledependentlocality") != col_trim_upper("or_friendly_doubledependentlocality_baabp"))
            | (col_trim_upper("or_friendly_locality") != col_trim_upper("or_friendly_locality_baabp"))
            | (col_trim_upper("or_friendly_posttown") != col_trim_upper("or_friendly_posttown_baabp"))
            | (col_trim_upper("or_friendly_postoutcode") != col_trim_upper("or_friendly_postoutcode_baabp"))
        )
    ) | (F.col("nad_key_baabp").isNull())

    df = df.withColumn("addr_modify_timestamp", F.when(addr_changed, ts_now).otherwise(F.lit("")))

    extracts_changed = (
        (F.col("nad_key_baabp").isNotNull())
        & (
            (col_trim_upper("exchange_group_code") != col_trim_upper("exchange_group_code_baabp"))
            | (col_trim_upper("district_code") != col_trim_upper("district_code_baabp"))
            | (col_trim("easting") != col_trim("easting_baabp"))
            | (col_trim("northing") != col_trim("northing_baabp"))
            | (col_trim("copper") != col_trim("copper_baabp"))
            | (col_trim("p2pfibre") != col_trim("p2pfibre_baabp"))
            | (col_trim("fttp_greenfield") != col_trim("fttp_greenfield_baabp"))
            | (col_trim("fttp_brownfield") != col_trim("fttp_brownfield_baabp"))
            | (col_trim("udprn") != col_trim("udprn_baabp"))
            | (col_trim("site_identifier") != col_trim("site_identifier_baabp"))
            | (col_trim("site_classification_desc") != col_trim("site_classification_desc_baabp"))
            | (col_trim("delivery_point") != col_trim("delivery_point_baabp"))
            | (F.col("addr_modify_timestamp") != "")
        )
    ) | (F.col("nad_key_baabp").isNull())

    df = df.withColumn(
        "extracts_modify_timestamp",
        F.when(extracts_changed, ts_now).otherwise(F.coalesce(F.col("extracts_modify_timestamp_baabp"), F.lit(""))),
    )

    css_changed = (
        (F.col("nad_key_baabp").isNotNull())
        & (
            (col_trim("paf_css_sub_premises") != col_trim("paf_css_sub_premises_baabp"))
            | (col_trim("paf_css_premises_name") != col_trim("paf_css_premises_name_baabp"))
            | (col_trim("paf_css_thoroughfare_number") != col_trim("paf_css_thoroughfare_number_baabp"))
            | (col_trim("paf_css_thoroughfare_name") != col_trim("paf_css_thoroughfare_name_baabp"))
            | (col_trim("paf_css_locality") != col_trim("paf_css_locality_baabp"))
            | (col_trim("paf_css_post_town") != col_trim("paf_css_post_town_baabp"))
            | (col_trim("paf_css_postcode") != col_trim("paf_css_postcode_baabp"))
        )
    ) | (F.col("nad_key_baabp").isNull())

    df = df.withColumn("css_modify_timestamp", F.when(css_changed, ts_now).otherwise(F.coalesce(F.col("css_modify_timestamp_baabp"), F.lit(""))))

    ins_changed = (
        (F.col("nad_key_baabp").isNotNull())
        & (
            (col_trim("ins_sub_premises") != col_trim("ins_sub_premises_baabp"))
            | (col_trim("ins_premises_name") != col_trim("ins_premises_name_baabp"))
            | (col_trim("ins_thoroughfare_number") != col_trim("ins_thoroughfare_number_baabp"))
            | (col_trim("ins_thoroughfare_name") != col_trim("ins_thoroughfare_name_baabp"))
            | (col_trim("ins_locality") != col_trim("ins_locality_baabp"))
            | (col_trim("ins_post_town") != col_trim("ins_post_town_baabp"))
            | (col_trim("ins_postcode") != col_trim("ins_postcode_baabp"))
            | (col_trim("ins_level") != col_trim("ins_level_baabp"))
        )
    ) | (F.col("nad_key_baabp").isNull())

    df = df.withColumn("ins_modify_timestamp", F.when(ins_changed, ts_now).otherwise(F.coalesce(F.col("ins_modify_timestamp_baabp"), F.lit(""))))
    return df


# --------------------------------------------------------------------------------------
# Main job
# --------------------------------------------------------------------------------------
def main():
    # -------- Load base reference datasets --------
    logger.info("Reading NGU repo...")
    ngu_df = spark.read.parquet(ngu_repo_stg_loc)
    log_count(ngu_df, "NGU INPUT")

    # add suffix _ngu
    for c in ngu_df.columns:
        ngu_df = ngu_df.withColumnRenamed(c, f"{c}_ngu")

    # -------- Read inputs --------
    logger.info("Reading BA ABP repo input...")
    ba_abp_repo = spark.read.parquet(ba_abp_inp_path)
    log_count(ba_abp_repo, "BA_ABP INPUT")

    logger.info("Reading INS output...")
    ba_abp_ins_op = spark.read.parquet(ba_abp_ins_path)
    log_count(ba_abp_ins_op, "INS INPUT")

    logger.info("Reading BA ABP Unique existing delta...")
    ba_abp_unique_exist = spark.read.parquet(ba_abp_unique_exist_path)
    log_count(ba_abp_unique_exist, "BA_ABP_UNIQUE_EXIST INPUT")

    logger.info("Reading TPO out (currently not used like lambda comments)...")
    tpo_df = spark.read.parquet(tpo_out_path)
    log_count(tpo_df, "TPO INPUT")
    for c in tpo_df.columns:
        tpo_df = tpo_df.withColumnRenamed(c, f"{c}_tpo")

    logger.info("Reading force create and level repo...")
    force_create_and_level_repo = spark.read.parquet(force_create_and_level_loc)
    log_count(force_create_and_level_repo, "FORCE_CREATE_LEVEL INPUT")
    for c in force_create_and_level_repo.columns:
        force_create_and_level_repo = force_create_and_level_repo.withColumnRenamed(c, f"{c}_fcl")

    # -------- INS rename like lambda --------
    ba_abp_ins_op = ba_abp_ins_op \
        .withColumnRenamed("ins_sub_premise", "ins_sub_premises") \
        .withColumnRenamed("ins_premise", "ins_premises_name") \
        .withColumnRenamed("ins_town", "ins_post_town")

    # add suffix _ins like lambda
    for c in ba_abp_ins_op.columns:
        ba_abp_ins_op = ba_abp_ins_op.withColumnRenamed(c, f"{c}_ins")

    # -------- Fetch INS columns (inner join) --------
    ba_abp_repo = ba_abp_repo.join(
        ba_abp_ins_op,
        (ba_abp_repo["nad_key"] == ba_abp_ins_op["nad_key_ins"])
        & (ba_abp_repo["type"] == ba_abp_ins_op["type_ins"])
        & (ba_abp_repo["lpi_key"] == ba_abp_ins_op["lpi_key_ins"]),
        "inner"
    )
    log_count(ba_abp_repo, "After INS INNER JOIN")

    # drop lambda dropped cols
    drop_cols = ["nad_key_ins", "uprn_ins", "type_ins", "udprn_ins", "lpi_key_ins"]
    for c in drop_cols:
        if c in ba_abp_repo.columns:
            ba_abp_repo = ba_abp_repo.drop(c)

    # rename _ins suffix away
    for c in ba_abp_repo.columns:
        if c.endswith("_ins"):
            ba_abp_repo = ba_abp_repo.withColumnRenamed(c, c.replace("_ins", ""))

    ba_abp_repo = ba_abp_repo.withColumn("ins_room", F.lit(""))

    # -------- paf_css_sub_premises logic --------
    css_mask = mask_for_css_subprem_and_css_prem_expr()

    cond_first = (
        (col_trim("uprn") != "") & (col_trim("udprn") == "") & (col_trim("inherited_udprn") != "")
        & (col_trim("lpi_summary_premisesname") != "") & (css_mask)
    )
    cond_second = (
        (col_trim("uprn") != "") & (col_trim("udprn") != "") & (col_trim("lpi_summary_subpremisesname") == "")
        & (col_trim("or_friendly_buildingname") != "") & (col_trim("paf_or_friendly_subpremise") != "")
    )

    ba_abp_repo = ba_abp_repo.withColumn(
        "paf_css_sub_premises",
        F.when(cond_first | cond_second, F.lit("")).otherwise(F.coalesce(F.col("paf_css_sub_premises"), F.lit(""))),
    )

    # -------- paf_css_premises_name logic --------
    cond_prem_1 = (
        (col_trim("uprn") != "") & (col_trim("udprn") != "") & (col_trim("lpi_summary_subpremisesname") == "")
        & (col_trim("or_friendly_buildingname") != "") & (col_trim("paf_or_friendly_subpremise") != "")
    )

    cond_prem_2 = (
        (col_trim("uprn") != "") & (col_trim("udprn") == "") & (col_trim("inherited_udprn") != "")
        & (col_trim("lpi_summary_premisesname") != "") & (col_trim("lpi_summary_subpremisesname") != "")
        & (css_mask)
    )

    ba_abp_repo = ba_abp_repo.withColumn(
        "paf_css_premises_name",
        F.when(
            cond_prem_1,
            F.concat_ws(" ", F.coalesce(F.col("paf_or_friendly_subpremise"), F.lit("")), F.coalesce(F.col("or_friendly_buildingname"), F.lit(""))),
        ).when(
            cond_prem_2,
            F.coalesce(F.col("lpi_summary_subpremisesname"), F.lit("")),
        ).otherwise(F.coalesce(F.col("paf_css_premises_name"), F.lit(""))),
    )

    # HISTORIC: "FORMER ..." rules
    is_historic = (F.upper(F.col("type")) == "LPI-HISTORIC")
    tf_no = col_trim("paf_css_thoroughfare_number")
    prem_name = col_trim("paf_css_premises_name")

    ba_abp_repo = ba_abp_repo.withColumn(
        "paf_css_premises_name",
        F.when(is_historic & (tf_no != "") & (prem_name != ""), F.concat(F.lit("FORMER "), tf_no, F.lit(" "), prem_name))
        .when(is_historic & (tf_no == "") & (prem_name != ""), F.concat(F.lit("FORMER "), prem_name))
        .when(is_historic & (tf_no != "") & (prem_name == ""), F.concat(F.lit("FORMER "), tf_no))
        .otherwise(F.col("paf_css_premises_name")),
    )

    ba_abp_repo = ba_abp_repo.withColumn(
        "paf_css_thoroughfare_number",
        F.when(is_historic, F.lit("")).otherwise(col_trim("paf_css_thoroughfare_number")),
    )

    ba_abp_repo = ba_abp_repo.withColumn(
        "paf_css_county",
        F.when((F.upper(col_trim("paf_css_post_town")) == "NEWPORT") & (col_trim("district_code") == "SW"), col_trim("paf_css_county")).otherwise(F.lit("")),
    )

    # Make all the thoroughfare number as whole integers (remove trailing .0)
    for c in [
        "thoroughfare_number",
        "or_friendly_thoroughfarenumber",
        "lpi_summary_premisesnumber",
        "lpi_or_friendly_thoroughfarenumber",
        "paf_or_friendly_thoroughfarenumber",
        "paf_css_thoroughfare_number",
        "paf_thoroughfare_number",
        "ins_thoroughfare_number",
    ]:
        if c in ba_abp_repo.columns:
            ba_abp_repo = ba_abp_repo.withColumn(c, strip_dot_zero_str(c))

    ba_abp_repo = ba_abp_repo.dropDuplicates()
    log_count(ba_abp_repo, "After BA_ABP dedup")

    # Store BA ABP Delta repository.
    logger.info(f"Writing BA_ABP Delta repo to: {ba_abp_repo_op_path}")
    ba_abp_repo.write.mode("overwrite").parquet(ba_abp_repo_op_path)

    # Assign address priority + dedup_rnk
    ba_abp_with_addr_priority = assign_address_priority(ba_abp_repo)

    w = Window.partitionBy("nad_key").orderBy("address_priority_new", "language_priority", "lpi_key")
    ba_abp_with_addr_priority = ba_abp_with_addr_priority.withColumn("dedup_rnk", F.row_number().over(w))
    log_count(ba_abp_with_addr_priority, "After address_priority + dedup_rnk")

    # Store for Gold Alternate address
    logger.info(f"Writing BA_ABP with addr priority to: {ba_abp_with_addr_priority_op_path}")
    ba_abp_with_addr_priority.write.mode("overwrite").parquet(ba_abp_with_addr_priority_op_path)

    # -------- Build unique repo pipeline --------
    ba_abp_unique_repo = ba_abp_with_addr_priority

    postcode_cols = ["or_friendly_postcode", "or_friendly_postincode", "or_friendly_postoutcode"]
    or_friendly_cols = [c for c in ba_abp_unique_repo.columns if c.startswith("or_friendly_") and c not in postcode_cols]

    # initcaps on non-postcode or_friendly cols
    for c in or_friendly_cols:
        ba_abp_unique_repo = ba_abp_unique_repo.withColumn(c, initcaps_expr(c))

    # child list join
    ba_abp_unique_repo = get_child_nad_and_uprn_list(ba_abp_unique_repo)
    if "nad_key_child" in ba_abp_unique_repo.columns:
        ba_abp_unique_repo = ba_abp_unique_repo.drop("nad_key_child")

    ba_abp_unique_repo = ba_abp_unique_repo.dropDuplicates()
    log_count(ba_abp_unique_repo, "After child join + initcaps")

    # force create and level updates
    ba_abp_with_force_create_and_level = ba_abp_unique_repo.join(
        force_create_and_level_repo,
        ba_abp_unique_repo["nad_key"] == force_create_and_level_repo["nad_key_fcl"],
        "left"
    )

    ba_abp_with_force_create_and_level = ba_abp_with_force_create_and_level.withColumn(
        "level",
        F.when(
            (F.col("nad_key_fcl").isNotNull()) & (F.col("update_level_flag_fcl") == "Y"),
            F.col("level_fcl")
        ).otherwise(F.col("level"))
    )

    ba_abp_with_force_create_and_level = ba_abp_with_force_create_and_level.withColumn(
        "force_create_flag",
        F.when(
            (F.col("nad_key_fcl").isNotNull()) & (F.col("update_force_create_flag_fcl") == "Y"),
            F.col("force_create_flag_fcl")
        ).otherwise(F.lit(""))
    )

    # drop _fcl columns
    for c in list(ba_abp_with_force_create_and_level.columns):
        if c.endswith("_fcl"):
            ba_abp_with_force_create_and_level = ba_abp_with_force_create_and_level.drop(c)

    ba_abp_unique_repo_1 = ba_abp_with_force_create_and_level.withColumn("or_friendly_county", F.lit(""))
    log_count(ba_abp_unique_repo_1, "After force_create + level update")

    # NGU merge: set req_type, promoted_type, lpia_key
    ba_abp_with_ngu = ba_abp_unique_repo_1.join(
        ngu_df,
        ba_abp_unique_repo_1["nad_key"] == ngu_df["nad_key_ngu"],
        "left"
    )

    ba_abp_with_ngu = ba_abp_with_ngu.withColumn("req_type", F.when(F.col("nad_key_ngu").isNotNull(), F.col("req_type_ngu")).otherwise(F.lit("")))
    ba_abp_with_ngu = ba_abp_with_ngu.withColumn("promoted_type", F.when(F.col("nad_key_ngu").isNotNull(), F.col("promoted_type_ngu")).otherwise(F.lit("")))
    ba_abp_with_ngu = ba_abp_with_ngu.withColumn("lpia_key", F.when(F.col("nad_key_ngu").isNotNull(), F.col("lpia_key_ngu")).otherwise(F.lit("")))
    ba_abp_with_ngu = ba_abp_with_ngu.withColumn(
        "promoted_type",
        F.when(F.upper(col_trim("promoted_type")) == "PAF", F.lit("PAF_retain")).otherwise(col_trim("promoted_type"))
    )

    # drop _ngu columns
    for c in list(ba_abp_with_ngu.columns):
        if c.endswith("_ngu"):
            ba_abp_with_ngu = ba_abp_with_ngu.drop(c)

    ba_abp_unique_repo_2 = ba_abp_with_ngu
    log_count(ba_abp_unique_repo_2, "After NGU merge (req/promote/lpia)")

    # -------- Read SSM config lists (exactly like lambda) --------
    def ssm_get_list(param_name_env_value):
        if not param_name_env_value:
            return []
        val = ssm_client.get_parameter(Name=param_name_env_value)["Parameter"]["Value"]
        return [x.strip() for x in val.split(",") if x.strip()]

    def ssm_get_json_map(param_name_env_value):
        if not param_name_env_value:
            return {}
        val = ssm_client.get_parameter(Name=param_name_env_value)["Parameter"]["Value"]
        return json.loads(val)

    ba_abp_col_list = ssm_get_list(ssm_ba_abp_col_list_param_name)
    dpa_col_list = ssm_get_list(ssm_dpa_col_list_param_name)
    oaa_col_list = ssm_get_list(ssm_oaa_col_list_param_name)

    # Calculate columns based on PROMOTE/DEMOTE
    ba_abp_unique_repo_3 = handle_promote_or_demote(ba_abp_unique_repo_2, ba_abp_col_list, oaa_col_list, dpa_col_list)
    log_count(ba_abp_unique_repo_3, "After handle_promote_or_demote")

    # -------- Top priority record picking (groupBy agg) --------
    ba_abp_unique_schema = ssm_get_list(ssm_ba_abp_unique_schema_param_name)
    agg_param1 = ssm_get_json_map(ssm_ba_abp_unique_agg_param1_param_name)
    agg_param2 = ssm_get_json_map(ssm_ba_abp_unique_agg_param2_param_name)
    merged_agg = {}
    merged_agg.update(agg_param1 or {})
    merged_agg.update(agg_param2 or {})

    # merged_agg values are like "max"/"sum"
    ba_abp_unique_repo = fetch_unique_record(ba_abp_unique_repo_3, merged_agg)
    log_count(ba_abp_unique_repo, "After fetch_unique_record (groupBy nad_key)")

    # -------- Existing repo join for transaction type --------
    ba_abp_unique_exist_df = ba_abp_unique_exist.dropDuplicates().na.fill("")
    # add suffix _baabp
    for c in ba_abp_unique_exist_df.columns:
        ba_abp_unique_exist_df = ba_abp_unique_exist_df.withColumnRenamed(c, f"{c}_baabp")

    join_df = ba_abp_unique_repo.join(
        ba_abp_unique_exist_df,
        ba_abp_unique_repo["nad_key"] == ba_abp_unique_exist_df["nad_key_baabp"],
        "left"
    )
    log_count(join_df, "After joining with existing (_baabp)")

    matched = join_df.filter(F.col("nad_key_baabp").isNotNull())
    unmatched = join_df.filter(F.col("nad_key_baabp").isNull())

    no_change = matched.filter(
        (col_trim_upper("district_code") == col_trim_upper("district_code_baabp"))
        & (col_trim_upper("exchange_group_code") == col_trim_upper("exchange_group_code_baabp"))
    ).withColumn("transaction_type", F.lit("NC"))

    update = matched.filter(
        (col_trim_upper("district_code") != col_trim_upper("district_code_baabp"))
        | (col_trim_upper("exchange_group_code") != col_trim_upper("exchange_group_code_baabp"))
    ).withColumn("transaction_type", F.lit("U2"))

    unmatched = unmatched.withColumn("transaction_type", F.lit("N"))

    ba_abp_unique_repo_final = no_change.unionByName(update).unionByName(unmatched)
    log_count(ba_abp_unique_repo_final, "After transaction_type derivation (NC/U2/N)")

    # Make thoroughfare number columns empty if value '0'
    for c in ["or_friendly_thoroughfarenumber", "paf_css_thoroughfare_number", "ins_thoroughfare_number"]:
        if c in ba_abp_unique_repo_final.columns:
            ba_abp_unique_repo_final = ba_abp_unique_repo_final.withColumn(c, F.when(F.col(c) == "0", F.lit("")).otherwise(F.col(c)))

    # Round/format easting/northing columns
    for c in ["easting", "northing", "oaa_easting", "oaa_northing"]:
        if c in ba_abp_unique_repo_final.columns:
            ba_abp_unique_repo_final = ba_abp_unique_repo_final.withColumn(c, to_num_round_format_str(c, 1))

    # Calculate timestamps
    ba_abp_unique_repo_final = calculate_timestamps(ba_abp_unique_repo_final)
    log_count(ba_abp_unique_repo_final, "After calculate_timestamps")

    # drop _baabp columns
    for c in list(ba_abp_unique_repo_final.columns):
        if c.endswith("_baabp"):
            ba_abp_unique_repo_final = ba_abp_unique_repo_final.drop(c)

    # initcaps again on or_friendly_cols (non-postcode)
    for c in or_friendly_cols:
        if c in ba_abp_unique_repo_final.columns:
            ba_abp_unique_repo_final = ba_abp_unique_repo_final.withColumn(c, initcaps_expr(c))

    # Select schema from SSM
    for c in ba_abp_unique_schema:
        if c not in ba_abp_unique_repo_final.columns:
            ba_abp_unique_repo_final = ba_abp_unique_repo_final.withColumn(c, F.lit(""))

    ba_abp_unique_repo_final = ba_abp_unique_repo_final.select(*ba_abp_unique_schema).dropDuplicates()
    log_count(ba_abp_unique_repo_final, "FINAL BA_ABP_UNIQUE_DELTA (post schema + dedup)")

    # -------- NC records from existing not in final --------
    final_keys = ba_abp_unique_repo_final.select("nad_key").dropDuplicates()

    ba_abp_nc_recs = ba_abp_unique_exist_df.join(
        final_keys,
        ba_abp_unique_exist_df["nad_key_baabp"] == final_keys["nad_key"],
        "left_anti"
    )

    # remove _baabp suffix
    for c in list(ba_abp_nc_recs.columns):
        if c.endswith("_baabp"):
            ba_abp_nc_recs = ba_abp_nc_recs.withColumnRenamed(c, c.replace("_baabp", ""))

    or_friendly_cols_nc = [c for c in ba_abp_nc_recs.columns if c.startswith("or_friendly_") and c not in postcode_cols]
    for c in or_friendly_cols_nc:
        ba_abp_nc_recs = ba_abp_nc_recs.withColumn(c, initcaps_expr(c))

    ba_abp_nc_recs = ba_abp_nc_recs.withColumn("transaction_type", F.lit("NC"))

    for c in ba_abp_unique_schema:
        if c not in ba_abp_nc_recs.columns:
            ba_abp_nc_recs = ba_abp_nc_recs.withColumn(c, F.lit(""))

    ba_abp_nc_recs = ba_abp_nc_recs.select(*ba_abp_unique_schema).dropDuplicates()

    for c in ["easting", "northing", "oaa_easting", "oaa_northing"]:
        if c in ba_abp_nc_recs.columns:
            ba_abp_nc_recs = ba_abp_nc_recs.withColumn(c, to_num_round_format_str(c, 1))

    log_count(ba_abp_nc_recs, "FINAL NC RECORDS (existing not in new delta)")

    # -------- Write outputs --------
    logger.info(f"Writing BA_ABP_UNIQUE_DELTA to: {ba_abp_unique_delta_loc}")
    ba_abp_unique_repo_final.write.mode("overwrite").parquet(ba_abp_unique_delta_loc)

    logger.info(f"Writing BA_ABP_UNIQUE_NC_EXIST to: {ba_abp_unique_delta_exist_loc}")
    ba_abp_nc_recs.write.mode("overwrite").parquet(ba_abp_unique_delta_exist_loc)

    logger.info("SUCCESS: BA ABP and BA ABP UNIQUE delta are successfully generated!")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err = traceback.format_exc()
        logger.error(f"FAILED with exception:\n{err}")
        if sns_arn:
            try:
                sns_client.publish(
                    TargetArn=sns_arn,
                    Message=f"There is an error occurred in BA ABP Unique Repository process:\n {err}\n{str(e)}",
                    Subject=mail_subject
                )
            except Exception:
                logger.error("Also failed to publish SNS notification.")
        raise
