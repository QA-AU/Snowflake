# Final working version 
import pandas as pd
from datetime import datetime, timedelta

def perform_scd2_operations(df_existing, df_new, current_timestamp):
    """
    Perform Slowly Changing Dimension Type 2 (SCD2) operations on the provided dataframes.

    This function takes two dataframes: one representing existing records and the other representing new records.
    It performs the following operations:
    1. Identifies records that need to be updated (i.e., records that exist in both dataframes but have different values).
    2. Identifies records that need to be deleted (i.e., records that exist in the existing dataframe but not in the new dataframe).
    3. Identifies records that need to be inserted (i.e., records that exist in the new dataframe but not in the existing dataframe).
    4. Identifies records that reappear after being end-dated.
    5. Updates the end_date for records that need to be updated or deleted.
    6. Handles reappearing records by creating new records with updated start_date and end_date as None.
    7. Inserts new records and updated records as new versions into the final dataframe.

    Parameters:
    df_existing (pd.DataFrame): The existing records dataframe.
    df_new (pd.DataFrame): The new records dataframe.
    current_timestamp (datetime): The current timestamp used for marking deletions.

    Returns:
    tuple: A tuple containing the final dataframe, new records to add, updated records to add, records to delete, and reappearing records.
    """
    # Step 3: Identify records for update, insert, and delete
    df_merged = df_existing.merge(df_new, on='id', how='outer', suffixes=('_existing', '_new'), indicator=True)

    # Identify records to update (any value changed and exists in new data)
    value_columns = ['value1', 'value2']
    update_condition = (df_merged['_merge'] == 'both') & df_merged[[f'{col}_existing' for col in value_columns]].ne(df_merged[[f'{col}_new' for col in value_columns]]).any(axis=1)
    records_to_update = df_merged[update_condition]

    # Identify records to delete (exists in existing data but not in new data)
    records_to_delete = df_merged[df_merged['_merge'] == 'left_only']

    # Identify records to insert (new records that don't exist in existing data)
    new_records = df_new[~df_new['id'].isin(df_existing['id'])]

    # Identify records that reappear after being end-dated
    reappearing_records = df_existing[(df_existing['end_date'].notna()) & (df_existing['id'].isin(df_new['id']))]

    # Step 4: Update records
    df_existing.loc[df_existing['id'].isin(records_to_update['id']), 'end_date'] = records_to_update['timestamp']

    # Step 5: Mark records as deleted
    df_existing.loc[df_existing['id'].isin(records_to_delete['id']), 'end_date'] = current_timestamp

    # Step 6: Handle reappearing records
    for index, row in reappearing_records.iterrows():
        new_record = {
            'id': row['id'],
            'start_date': df_new.loc[df_new['id'] == row['id'], 'timestamp'].values[0],
            'end_date': None
        }
        for col in value_columns:
            new_record[col] = df_new.loc[df_new['id'] == row['id'], col].values[0]
        df_existing = pd.concat([df_existing, pd.DataFrame([new_record])], ignore_index=True)

    # Step 7: Insert new records
    new_records_to_add = new_records[['id'] + value_columns].copy()
    new_records_to_add['start_date'] = new_records['timestamp']
    new_records_to_add['end_date'] = None

    # Step 8: Insert updated records as new versions
    updated_records_to_add = records_to_update[['id'] + [f'{col}_new' for col in value_columns]].copy()
    updated_records_to_add.columns = ['id'] + value_columns
    updated_records_to_add['start_date'] = records_to_update['timestamp']
    updated_records_to_add['end_date'] = None

    # Combine existing, new, and updated records
    df_final = pd.concat([df_existing, new_records_to_add, updated_records_to_add], ignore_index=True)
    df_final.sort_values(by='id', inplace=True)

    return df_final, new_records_to_add, updated_records_to_add, records_to_delete, reappearing_records

# Example usage
if __name__ == "__main__":
    # Initial data with multiple columns
    initial_data = {
        'id': [1, 2, 3, 5],
        'value1': ['A', 'B', 'C', 'E'],
        'value2': [10, 20, 30, 40],
        'start_date': [datetime(2020, 1, 1), datetime(2020, 1, 1), datetime(2020, 1, 1), datetime(2020, 1, 1)],
        'end_date': [None, None, None, datetime(2021, 4, 1)]
    }
    df_existing = pd.DataFrame(initial_data)

    # Current data with multiple columns
    new_data = {
        'id': [1, 2, 4, 5],
        'value1': ['A1', 'B', 'D', 'E'],
        'value2': [15, 20, 35, 40],
        'timestamp': [datetime(2021, 6, 1), datetime(2021, 6, 1), datetime(2021, 6, 1), datetime(2021, 6, 1)]
    }
    df_new = pd.DataFrame(new_data)

    # Calculate 2 seconds before midnight
    now = datetime.now()
    midnight = datetime.combine(now, datetime.max.time())
    current_timestamp = (midnight - timedelta(seconds=2)).replace(microsecond=0)

    df_final, new_records_to_add, updated_records_to_add, records_to_delete, reappearing_records = perform_scd2_operations(df_existing, df_new, current_timestamp)

    # Print initial data and new data
    print("\nInitial data:")
    print(df_existing)

    print("\nNew data:")
    print(df_new)

    print("\nSCD2 Analysis :")

    # Display the final dataframe
    print("\nNew records to add:")
    print(new_records_to_add)

    print("\nUpdated records to add:")
    print(updated_records_to_add)

    print("\nRecords to delete:")
    print(records_to_delete)

    print("\nSoftdeleted/Reappearing records:")
    print(reappearing_records)

    print("\nFinal SCD2 records:")
    print(df_final)
