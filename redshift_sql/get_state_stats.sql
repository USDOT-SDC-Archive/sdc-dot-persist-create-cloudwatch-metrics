select a.state,b.table_name,a.total_rows_ingested from etl_waze.elt_run_state_stats a,etl_waze.DW_TBL_INFO b where etl_run_id={{ batchIdValue }} and b.table_id=a.table_id

