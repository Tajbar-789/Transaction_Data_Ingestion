import os
import glob
import logging
import pandas as pd

# Configurable settings dictionary with updated input/output paths
CONFIG = {
    "idempotent_transaction_pipeline": {
        "input_dir": "./input",
        "input_file_format": "*.csv",
        "output_dir": "./output",
        "clean_file_path": "./output/transaction_valid_final_file.csv",
        "quarantine_file_path": "./output/quarantine_file.csv",
        "summary_file": "./output/summary.txt",
        "primary_key": "ref_no",
        "date_col": "txn_date",
        "time_col": "txn_time",
        "sort_cols": ['account_number', 'txn_date', 'txn_time']
    }
}

class Dataingestion_Pipeline:
    def __init__(self, config, pipeline_name):
        # Load configuration dynamically
        self.cfg = config[pipeline_name]
        self.clean_file_path = self.cfg["clean_file_path"]
        self.primary_key = self.cfg["primary_key"]
        
        # Ensure output directory exists
        os.makedirs(self.cfg["output_dir"], exist_ok=True)
        
    def run(self):
        # reading all the csv files into dataframe
        # Dynamically loading files instead of hardcoding df1, df2, df3
        
        file_pattern = self.cfg.get("input_file_format", "*.csv")
        all_files = glob.glob(os.path.join(self.cfg["input_dir"], file_pattern))
        all_files = [f for f in all_files if 'transaction_valid_final_file' not in f]
        
        dfs = []
        for file in all_files:
            temp_df = pd.read_csv(file)
            dfs.append(temp_df)
            
        # concatenate all the csv file into a dataframe  
        df=pd.concat(dfs,axis=0,ignore_index=True)

        # converting txn_date and txn_time into a single format 
        df['txn_time']=pd.to_datetime(df['txn_time'],format='mixed', errors='coerce').dt.strftime('%H:%M:%S')
        df['txn_date']=pd.to_datetime(df['txn_date'],format='mixed', errors='coerce').dt.strftime('%Y-%m-%d')

        ## removing duplicates from dataframe using ref_no as the unique key since each transaction will have a unique ref_no to identify individual records  
        df_deduped = df.drop_duplicates([self.primary_key],keep='last',ignore_index=True)
        no_of_duplicates_removed=len(df)-len(df_deduped)

        ## removing records with NULL ref_no since transaction with no ref_no does not make sense 
        ref_no_null=df_deduped[self.primary_key].isna()

        ## identifying records with mentioned issues 
        invalid_txn_date_records=df_deduped[self.cfg["date_col"]].isna()
        records_with_both_credit_and_debit_present=(df_deduped.credit.notna()) & (df_deduped.debit.notna()) 
        records_with_both_credit_and_debit_missing=(df_deduped.credit.isna()) & (df_deduped.debit.isna()) 

        #isolating records with issues 
        df_deduped['rejection_reason'] = None
        df_deduped.loc[invalid_txn_date_records, 'rejection_reason'] = "Invalid transaction date"
        df_deduped.loc[records_with_both_credit_and_debit_present, 'rejection_reason'] = "Both debit and credit present"
        df_deduped.loc[records_with_both_credit_and_debit_missing, 'rejection_reason'] = "Neither debit nor credit present"
        df_deduped.loc[ref_no_null, 'rejection_reason'] = "Missing primary key (ref_no)"

        quarantine_df = df_deduped[df_deduped['rejection_reason'].notna()]
        valid_df = df_deduped[df_deduped['rejection_reason'].isna()].drop(columns=['rejection_reason'])

        valid_df.sort_values(by=self.cfg["sort_cols"],inplace=True,ignore_index=True)

        # Upsert logic starting
        if os.path.exists(self.clean_file_path):
            existing_df = pd.read_csv(self.clean_file_path)
            logging.info(f"Loaded {len(existing_df)} records from previous runs.")
            
            #  COMBINE: Stack the old data and the new data together
            combined_df = pd.concat([existing_df, valid_df], ignore_index=True)
        else:
            # First time the pipeline is ever run
            combined_df = valid_df.copy()

        #  SORT: Ensure the newest/highest priority records are at the bottom
        combined_df = combined_df.sort_values(by=['account_number','txn_date','txn_time'])

        #  UPSERT (DEDUPLICATE): Drop duplicates based on the ref_no , keeping the last row
        # - If 'ref_no' is new, it gets inserted.
        # - If 'ref_no' exists, the older record is dropped and the new one updates it.
        final_clean = combined_df.drop_duplicates(subset=[self.primary_key], keep='last')

        #  OVERWRITE: Save the new state back to disk (happens in your run() method)
        final_clean.to_csv(self.clean_file_path, index=False)

        quarantine_df.to_csv(self.cfg["quarantine_file_path"],index=False)

        # Reporting logic
        total_records_recieved=len(df)
        total_records_successfully_processed=len(valid_df)
        total_records_rejected=len(quarantine_df)
        reason_for_rejected_records=pd.unique(quarantine_df['rejection_reason'])
        
        # Dynamic calculation of late correction impact
        late_files = [f for f in all_files if 'late_correction' in f]
        late_count = sum([len(pd.read_csv(f)) for f in late_files]) if late_files else 0
        impact_of_late_correction_file=str(late_count)+" records were added to the earlier data and deduped based on ref_no and processed later.The end result is sorted based on account_number,txn_date and txn_time to place the late arrived data in correct order "

        with open(self.cfg["summary_file"],'w') as f :
            f.write(f"total_records_recieved : {total_records_recieved}\n")
            f.write(f"total_records_successfully_processed : {total_records_successfully_processed}\n")
            f.write(f"total_records_rejected : {total_records_rejected}\n")
            f.write(f"reason_for_rejected_records : {reason_for_rejected_records}\n")        
            f.write(f"no_of_duplicates_removed : {no_of_duplicates_removed}\n")
            f.write(f"impact_of_late_correction_file : {impact_of_late_correction_file}")  

if __name__ == "__main__":
    pipeline = Dataingestion_Pipeline(config=CONFIG, pipeline_name="idempotent_transaction_pipeline")
    pipeline.run()