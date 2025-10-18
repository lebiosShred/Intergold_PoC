import pandas as pd
import io

def process_intergold_query(
    file_content: bytes,
    metric_column: str,
    order_type: str,
    group_by_columns: list
) -> str:
    """
    Loads Inter Gold data from an Excel file in memory, applies all business rules,
    and returns the aggregated result as a JSON string.
    """
    
    # 1. Load Data from Memory using the correct engine for .xlsx files
    df = pd.read_excel(io.BytesIO(file_content), engine='openpyxl')

    # 2. Apply Business Rule A: Classify SO Description into LGD/Mined
    df['order_type_classified'] = df['SO Description'].apply(
        lambda x: 'LGD' if 'LGD' in str(x) else 'Mined'
    )

    # 3. Apply Business Rule B: Reclassify KT (Metal Karatage)
    kt_mapping = {
        '10K': '10K', '09K': '09K', '14K': '14K',
        '10K92': 'GOLD+SILVER', 'S925': 'SILVER', '09K92': 'GOLD+SILVER',
        '18K': '18K', 'U09K': '09K', 'PT95': 'PLATINUM',
        'U14K': '14K', '14K92': 'GOLD+SILVER', 'S930': 'SILVER',
        'U18K': '18K', '09KPT': 'GOLD+PLATINUM', 'S928': 'SILVER',
        'U10K': '10K', 'BRASS': 'BRASS'
    }
    df['KT_reclassified'] = df['KT'].map(kt_mapping).fillna(df['KT'])

    # 4. Apply Business Rule C: Define Custom Sort Order for PPC Delivery Period
    # This ensures the output is sorted exactly as the client requested.
    ppc_order_base = ['To Check', 'Overdue', 'Due']
    
    # Dynamically find all 'Week' columns and sort them in descending order
    all_weeks = sorted(
        [period for period in df['PPC Delivery Period'].unique() if isinstance(period, str) and 'W' in period], 
        reverse=True
    )
    
    full_ppc_order = ppc_order_base + all_weeks
    
    df['PPC Delivery Period'] = pd.Categorical(
        df['PPC Delivery Period'], categories=full_ppc_order, ordered=True
    )
    
    # 5. Filter the DataFrame based on the user's query (LGD or Mined)
    df_filtered = df[df['order_type_classified'] == order_type].copy()

    # 6. Group by the requested attributes and aggregate the metric
    # The 'observed=True' argument is good practice for performance with categorical data.
    final_grouping_columns = group_by_columns + ['PPC Delivery Period']
    result_df = df_filtered.groupby(final_grouping_columns, observed=True).agg(
        total=(metric_column, 'sum')
    ).reset_index()

    # 7. Sort the final results based on the custom PPC order
    result_df = result_df.sort_values(by='PPC Delivery Period')
    
    # 8. Convert the final DataFrame to a JSON string for the API response
    return result_df.to_json(orient='records')