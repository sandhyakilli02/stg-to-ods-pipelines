"""
KAN-37 — ENHANCEMENT DELTA — salesiq_siqods_trade_plcy_mastr (PLCY_MASTR)
Run ID: 3ce5b7b9-5198-4e71-8295-2053690b86d5

Scope: PLCY_MASTR.PLCY_SOLD_STATUS_INDCTR moves from a hardcoded literal 'P'
to a direct pull from STG_SALESIQ_TRADE_EVENT_STS.PLCY_SOLD_STATUS_INDCTR.
No other PLCY_MASTR column, key, grain, or the parallel
salesiq_siqods_customer_plcy_mastr load path is touched by this change.

Production architecture note (per knowledgebase/existing_pipeline_standards.md):
the shared Glue engine `stg-ods.py` is config-driven — all per-table SQL lives in
`stg-ods.json`. The DataFrame-API functions below are a self-contained,
STTM-annotated representation of that SQL logic (for review / QA-harness use);
the actual production change is the JSON delta in MERGE INSTRUCTIONS.
"""

# =============================================================================
# IMPACT ANALYSIS
# =============================================================================
# stg-ods.py  : No change required. `plcy_mastr` (salesiq_siqods_trade_plcy_mastr)
#               has no table-specific Python branch in load_ods_table() /
#               read_source_file() (special-cased tables are only
#               invstmt_fund_trx, invstmt_fund_asset, pty_detl, plcy_trx_sumry —
#               see existing_pipeline_standards.md). CONF_LKP_QUERY and
#               CONF_SRC_FLTR_QUERY are read generically from JSON_CONTENT and
#               executed via spark.sql(); the engine has no awareness of which
#               columns those queries project.
# stg-ods.json: salesiq_siqods_trade_plcy_mastr.src_fltr_query  — MUST change
#               salesiq_siqods_trade_plcy_mastr.lkp_query       — MUST change
#               salesiq_siqods_trade_plcy_mastr.select_fields_query — No change
#               (already selects `plcy_sold_status_indctr` from tgt_sink_df_vw).
# Reason      : Inspecting the live config (stg-ods.json), `src_fltr_query` for
#               this table is currently
#                 "SELECT SRC_SYS,ACCT_NUM  FROM src_file_vw"
#               which projects the source file down to only SRC_SYS and
#               ACCT_NUM before it is registered as `src_vw` — the view
#               `lkp_query` reads from. PLCY_SOLD_STATUS_INDCTR is dropped at
#               this step today. Simply editing `lkp_query` to reference
#               `src.PLCY_SOLD_STATUS_INDCTR` would fail at runtime
#               (column not found on src_vw) unless `src_fltr_query` is also
#               widened to carry the column through. This is a non-obvious
#               two-key change the Enhancement standards' single-line example
#               (KAN-48) does not call out explicitly — flagging it here so
#               the merge does not silently break at execution.
#
# Enhancement Checklist (existing_pipeline_standards.md):
# [x] 1. JSON config key holding the change: BOTH `src_fltr_query` (must widen
#        the projection to carry PLCY_SOLD_STATUS_INDCTR through to src_vw)
#        AND `lkp_query` (column derivation: literal 'P' -> src column).
#        No new lkp_table entry is needed — STG_SALESIQ_TRADE_EVENT_STS.csv is
#        already the job's own source file (event_src_file_name), not a
#        separate lookup table, so no join clause is required: PLCY_NUM is
#        already derived from the same source row (`src.ACCT_NUM as PLCY_NUM`),
#        so PLCY_SOLD_STATUS_INDCTR from that same row satisfies the Data
#        Model's SRC_SYS+ACCT_NUM = SRC_SYS+PLCY_NUM relationship by construction.
# [x] 2. Python file change needed? NO. No new Glue arg, no new DQ/alert
#        branch, no new table-specific block — `stg-ods.py: no change required`.
# [x] 3. New lookup table? NO — see point 1.
# [x] 4. select_fields_query needs a new column? NO — `plcy_sold_status_indctr`
#        is already projected from `tgt_sink_df_vw` in the existing query.
# [x] 5. Does tgt_tbl_nk change? NO — natural key remains SRC_SYS,PLCY_NUM;
#        rec_after_fltr's left-join-null insert-only pattern is unaffected.

# =============================================================================
# MERGE INSTRUCTIONS
# =============================================================================
# Apply both edits inside the `salesiq_siqods_trade_plcy_mastr` block of
# stg-ods.json ONLY. Do NOT touch the sibling `salesiq_siqods_customer_plcy_mastr`
# block (out of scope per KAN-37 scope guard).
#
# 1) src_fltr_query  — widen projection to carry the status column through:
#    OLD:
#      "SELECT SRC_SYS,ACCT_NUM  FROM src_file_vw "
#    NEW:
#      "SELECT SRC_SYS,ACCT_NUM,PLCY_SOLD_STATUS_INDCTR FROM src_file_vw "
#
# 2) lkp_query — replace the hardcoded literal with the direct source pull:
#    OLD:
#      "select src.SRC_SYS, 'SS' as SRC_ADMN_SYS, 'N/A' as PLCY_COMP_CD, "
#      "src.ACCT_NUM as PLCY_NUM, src.ACCT_NUM as LEFT_JUSTFD_PLCY_NUM, "
#      "'N/A' as INVSTMT_COMP_TAX_ID, -99 as CASE_MASTR_ID, "
#      "'P' as PLCY_SOLD_STATUS_INDCTR, '-99' as DEL_INDCTR from src_vw src"
#    NEW:
#      "select src.SRC_SYS, 'SS' as SRC_ADMN_SYS, 'N/A' as PLCY_COMP_CD, "
#      "src.ACCT_NUM as PLCY_NUM, src.ACCT_NUM as LEFT_JUSTFD_PLCY_NUM, "
#      "'N/A' as INVSTMT_COMP_TAX_ID, -99 as CASE_MASTR_ID, "
#      "src.PLCY_SOLD_STATUS_INDCTR as PLCY_SOLD_STATUS_INDCTR, "
#      "'-99' as DEL_INDCTR from src_vw src"
#
# 3) select_fields_query — no change (already selects plcy_sold_status_indctr).
#
# 4) stg-ods.py — no change; no function/line to update.
#
# The two functions below are the DataFrame-API equivalent of edits (1) and (2),
# provided for code review / QA-harness parity checks against the SQL delta —
# they are not a separate execution path in production.

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def transform_trade_event_sts_src_filter(src_file_df: DataFrame) -> DataFrame:
    """Project STG_SALESIQ_TRADE_EVENT_STS down to the columns the trade-event
    PLCY_MASTR load needs, now including PLCY_SOLD_STATUS_INDCTR.

    DataFrame-API equivalent of the updated
    salesiq_siqods_trade_plcy_mastr.src_fltr_query in stg-ods.json.
    """
    try:
        return (
            src_file_df
            # STTM Rule: PLCY_MASTR.SRC_SYS — direct copy from staging record;
            # forms part 1 of the natural key (tgt_tbl_nk = SRC_SYS,PLCY_NUM). No change.
            .select(
                F.col("SRC_SYS"),
                F.col("ACCT_NUM"),
                # STTM Rule: PLCY_MASTR.PLCY_SOLD_STATUS_INDCTR [CHANGED — KAN-37] —
                # carry PLCY_SOLD_STATUS_INDCTR through the source filter step so the
                # lookup query below can reference it directly (previously dropped here).
                F.col("PLCY_SOLD_STATUS_INDCTR"),
            )
        )
    except Exception as exc:
        logger.error(
            "Failed to project STG_SALESIQ_TRADE_EVENT_STS source filter columns: %s",
            exc,
            exc_info=True,
        )
        raise


def transform_plcy_mastr(src_vw_df: DataFrame) -> DataFrame:
    """Apply the salesiq_siqods_trade_plcy_mastr STTM mappings that build the
    lookup-view row for PLCY_MASTR (DataFrame-API equivalent of the updated
    lkp_query in stg-ods.json). Only PLCY_SOLD_STATUS_INDCTR's derivation
    changes under KAN-37; every other column is existing, unchanged production
    logic and is included here for a complete, self-contained mapping.
    """
    try:
        return (
            src_vw_df
            # STTM Rule: PLCY_MASTR.SRC_SYS — direct copy from staging record. No change.
            .withColumn("SRC_SYS", F.col("SRC_SYS"))
            # STTM Rule: PLCY_MASTR.SRC_ADMN_SYS — hardcoded constant literal 'SS'. No change.
            .withColumn("SRC_ADMN_SYS", F.lit("SS"))
            # STTM Rule: PLCY_MASTR.PLCY_COMP_CD — hardcoded constant literal 'N/A'. No change.
            .withColumn("PLCY_COMP_CD", F.lit("N/A"))
            # STTM Rule: PLCY_MASTR.PLCY_NUM — direct copy, PLCY_NUM = ACCT_NUM. No change.
            .withColumn("PLCY_NUM", F.col("ACCT_NUM"))
            # STTM Rule: PLCY_MASTR.LEFT_JUSTFD_PLCY_NUM — left-justified PLCY_NUM
            # (RPAD(TRIM(ACCT_NUM), 255, ' ')); production lkp_query currently emits the
            # untrimmed/unpadded value (src.ACCT_NUM as LEFT_JUSTFD_PLCY_NUM) — reproduced
            # as-is since KAN-37 does not request a change to this column.
            .withColumn("LEFT_JUSTFD_PLCY_NUM", F.col("ACCT_NUM"))
            # STTM Rule: PLCY_MASTR.INVSTMT_COMP_TAX_ID — hardcoded constant literal 'N/A'. No change.
            .withColumn("INVSTMT_COMP_TAX_ID", F.lit("N/A"))
            # STTM Rule: PLCY_MASTR.CASE_MASTR_ID — hardcoded constant literal -99. No change.
            .withColumn("CASE_MASTR_ID", F.lit(-99))
            # STTM Rule: PLCY_MASTR.PLCY_SOLD_STATUS_INDCTR [CHANGED — KAN-37] — direct
            # pull from STG_SALESIQ_TRADE_EVENT_STS.PLCY_SOLD_STATUS_INDCTR (same source
            # row as SRC_SYS/ACCT_NUM, satisfying the SRC_SYS+ACCT_NUM = SRC_SYS+PLCY_NUM
            # match by construction). Replaces the prior F.lit('P') hardcode. No casing,
            # truncation, or default-masking is applied — nulls/blanks pass through as-is
            # per the STTM's null-handling business rule.
            .withColumn("PLCY_SOLD_STATUS_INDCTR", F.col("PLCY_SOLD_STATUS_INDCTR"))
            # STTM Rule: PLCY_MASTR.DEL_INDCTR — hardcoded constant literal '-99'. No change.
            .withColumn("DEL_INDCTR", F.lit("-99"))
        )
    except Exception as exc:
        logger.error(
            "Failed to build PLCY_MASTR lookup-view row from src_vw: %s",
            exc,
            exc_info=True,
        )
        raise


def validate_plcy_sold_status_length(plcy_mastr_lkp_df: DataFrame, max_length: int = 50) -> None:
    """Data-quality check only — does not filter or mutate the DataFrame.

    STTM Rule: PLCY_MASTR.PLCY_SOLD_STATUS_INDCTR — data type/length check:
    values sourced from STG_SALESIQ_TRADE_EVENT_STS must fit within VARCHAR(50);
    any value exceeding this length must be flagged as a DQ exception rather
    than silently truncated. This pipeline has no per-row reject path for this
    column, so exceedances are logged for review instead of altering the row.
    """
    try:
        exceeding_cnt = plcy_mastr_lkp_df.filter(
            F.length(F.col("PLCY_SOLD_STATUS_INDCTR")) > max_length
        ).count()
        if exceeding_cnt > 0:
            logger.warning(
                "DQ exception: %d row(s) have PLCY_SOLD_STATUS_INDCTR longer than "
                "VARCHAR(%d) — flagged for review, not truncated.",
                exceeding_cnt,
                max_length,
            )
        else:
            logger.info("PLCY_SOLD_STATUS_INDCTR length check passed for all rows.")
    except Exception as exc:
        logger.error(
            "Failed to run PLCY_SOLD_STATUS_INDCTR length validation: %s",
            exc,
            exc_info=True,
        )
        raise
