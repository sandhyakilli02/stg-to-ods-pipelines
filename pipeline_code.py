# IMPACT ANALYSIS
# stg-ods.py  : No change required. `plcy_mastr` (target: PLCY_MASTR, catalog_tbl:
#               salesiq_siqods_plcy_mastr) has no table-specific Python block in
#               load_ods_table() / read_source_file() — it uses the fully generic
#               lkp_query -> rec_after_fltr -> select_fields_query flow described in
#               knowledgebase/existing_pipeline_standards.md. No new Glue job argument,
#               DQ/alert branch, lookup-table type, or load-type logic is introduced by
#               this derivation. This mirrors the KAN-48 precedent
#               (PLCY_SOLD_STATUS_INDCTR) documented in that same standards file.
# stg-ods.json: salesiq_siqods_trade_plcy_mastr.lkp_query, .select_fields_query
# Reason      : PLCY_ACTV_INDCTR is a new, intra-row derived column computed entirely
#               from PLCY_SOLD_STATUS_INDCTR, which is already staged on the same
#               lkp_vw row (sourced 1:1 from STG_SALESIQ_TRADE_EVENT_STS under KAN-37,
#               and already projected by src_fltr_query — verified so this isn't the
#               src_fltr_query column-projection trap). Adding a CASE WHEN expression
#               and its projection is pure SQL-in-config; no new join, lookup table,
#               Glue arg, or table-specific Python branch is needed.

# ── ENHANCEMENT DELTA — Enhancement for 77fa37be-4a9e-4608-b95d-2c8dab3b01a8 (KAN-38) ──
# Integrate these config changes into conduit/backend/stg-ods.json.
# This is a JSON-config-only enhancement — no PySpark function changes apply.
# (Both edits below have already been applied directly to
#  conduit/backend/stg-ods.json under the salesiq_siqods_trade_plcy_mastr entry.)

# --- lkp_query (BEFORE) ---
# "select src.SRC_SYS, 'SS' as SRC_ADMN_SYS, 'N/A' as PLCY_COMP_CD, src.ACCT_NUM as PLCY_NUM, "
# "src.ACCT_NUM as LEFT_JUSTFD_PLCY_NUM, 'N/A' as INVSTMT_COMP_TAX_ID, -99 as CASE_MASTR_ID, "
# "src.PLCY_SOLD_STATUS_INDCTR as PLCY_SOLD_STATUS_INDCTR, '-99' as DEL_INDCTR from src_vw src"

# --- lkp_query (AFTER) ---
NEW_LKP_QUERY = (
    "select src.SRC_SYS, 'SS' as SRC_ADMN_SYS, 'N/A' as PLCY_COMP_CD, src.ACCT_NUM as PLCY_NUM, "
    "src.ACCT_NUM as LEFT_JUSTFD_PLCY_NUM, 'N/A' as INVSTMT_COMP_TAX_ID, -99 as CASE_MASTR_ID, "
    "src.PLCY_SOLD_STATUS_INDCTR as PLCY_SOLD_STATUS_INDCTR, '-99' as DEL_INDCTR, "
    # STTM Rule: KAN-38 PLCY_ACTV_INDCTR CASE WHEN — derived intra-row from
    # src.PLCY_SOLD_STATUS_INDCTR (STG_SALESIQ_TRADE_EVENT_STS, sourced under KAN-37);
    # replaces the ticket-described "hardcoded 'Y'" gap (column previously did not exist).
    "/* STTM Rule: KAN-38 PLCY_ACTV_INDCTR CASE WHEN - derived from src.PLCY_SOLD_STATUS_INDCTR "
    "(STG_SALESIQ_TRADE_EVENT_STS, sourced under KAN-37); replaces prior no-column/hardcoded-'Y' gap */ "
    "CASE WHEN src.PLCY_SOLD_STATUS_INDCTR IN ('S','A') THEN 'Y' "
    "WHEN src.PLCY_SOLD_STATUS_INDCTR IN ('C','L','X','T') THEN 'N' "
    "ELSE 'P' END as PLCY_ACTV_INDCTR "
    "from src_vw src"
)

# --- select_fields_query (BEFORE) ---
# "SELECT  src_admn_sys, plcy_comp_cd, plcy_num, left_justfd_plcy_num,cast({0} as decimal(19,0)) "
# "as BTCH_ID, current_date as CRTD_DT, null as MDFD_DT, '{1}-trade' as CRTD_BY, null as MDFD_BY, "
# "case_mastr_id, src_sys, plcy_sold_status_indctr, del_indctr, invstmt_comp_tax_id FROM tgt_sink_df_vw"

# --- select_fields_query (AFTER) ---
NEW_SELECT_FIELDS_QUERY = (
    "SELECT  src_admn_sys, plcy_comp_cd, plcy_num, left_justfd_plcy_num,"
    "cast({0} as decimal(19,0)) as BTCH_ID, current_date as CRTD_DT, null as MDFD_DT, "
    "'{1}-trade' as CRTD_BY, null as MDFD_BY, case_mastr_id, src_sys, plcy_sold_status_indctr, "
    # STTM Rule: KAN-38 PLCY_ACTV_INDCTR CASE WHEN — final projection of the new column
    # computed in lkp_query above; must reach the target INSERT column list.
    "/* STTM Rule: KAN-38 PLCY_ACTV_INDCTR CASE WHEN - final projection */ plcy_actv_indctr, "
    "del_indctr, invstmt_comp_tax_id FROM tgt_sink_df_vw"
)

# ── MERGE INSTRUCTIONS ──────────────────────────────────────────────────────
# 1. File: conduit/backend/stg-ods.json
#    Key:  salesiq_siqods_trade_plcy_mastr.lkp_query
#    Change: append one derived column to the existing SELECT list (do not remove
#    any existing column) — add, immediately before "from src_vw src":
#       , CASE WHEN src.PLCY_SOLD_STATUS_INDCTR IN ('S','A') THEN 'Y'
#              WHEN src.PLCY_SOLD_STATUS_INDCTR IN ('C','L','X','T') THEN 'N'
#              ELSE 'P' END as PLCY_ACTV_INDCTR
#    (See NEW_LKP_QUERY above for the exact full string, with an inline
#    /* STTM Rule: KAN-38 ... */ SQL comment citing this mapping — JSON has no
#    native comment syntax, so the STTM rule citation is embedded as a SQL
#    block comment inside the query string itself, per this project's convention
#    for JSON-config-only changes.)
#
# 2. File: conduit/backend/stg-ods.json
#    Key:  salesiq_siqods_trade_plcy_mastr.select_fields_query
#    Change: add `plcy_actv_indctr` to the SELECT list (see NEW_SELECT_FIELDS_QUERY
#    above), immediately after `plcy_sold_status_indctr,` and before `del_indctr,`.
#
# 3. File: inputs/stg-ods.py
#    Change: NONE. Do not edit. `plcy_mastr` uses the fully generic lkp_query ->
#    rec_after_fltr -> select_fields_query flow; rec_after_fltr's `select lkp.*`
#    already carries PLCY_ACTV_INDCTR through once it exists in lkp_vw, with no
#    Python-side reference to individual column names required.
#
# 4. No change to lkp_tables, table_N, src_fltr_query, rec_after_fltr, or tgt_tbl_nk:
#    - src_fltr_query already projects PLCY_SOLD_STATUS_INDCTR (from the KAN-37 fix),
#      so no upstream column-projection change is needed for this derivation to see
#      its source column.
#    - tgt_tbl_nk (SRC_SYS,PLCY_NUM) is unchanged — this enhancement does not alter
#      the natural key.
#    - table_load_type remains "0" (insert-only / SCD Type 0) — no history/backfill
#      logic is introduced; pre-existing PLCY_MASTR rows are not retroactively
#      recomputed (flagged in the approved STTM as a business follow-up, out of
#      scope for KAN-38).
#
# Both edits above have already been applied to conduit/backend/stg-ods.json in this
# run's working copy at:
#   C:\Users\sipinisetty\Downloads\toolkit_template (2) 1\toolkit_template\conduit\backend\stg-ods.json
