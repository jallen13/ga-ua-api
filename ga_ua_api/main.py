import pandas as pd
from gaapi4py import GAClient

def fetch_ga_data(service_acount_path, view_id, start_date, end_date, metrics, dimensions):  
    c = GAClient(json_keyfile=service_acount_path)
    c.set_view_id(view_id)

    request_body = {
        'start_date': start_date,
        'end_date': end_date,
        'metrics': metrics,
        'dimensions': dimensions
    }

    response = c.get_all_data(request_body)

    return response

def anti_sample(service_acount_path, view_id, start_date, end_date, metrics, dimensions):
    print('Finding how much sampling in data request...')
    test_call = fetch_ga_data(service_acount_path, view_id, start_date, end_date, metrics, dimensions)

    # Reduce read counts by 10% to get more calls as returned figure is flakey
    read_counts = round(int(test_call['info']['samplesReadCounts'] or 0) * 0.9)
    space_size = int(test_call['info']['samplingSpaceSizes'] or 0)
    sampling_percent = read_counts / space_size if space_size else 0.0

    # Add 20% to rowCount as its flakey (sampled rows of 0 not included?)
    row_count = round(int(test_call['info']['rowCount'] or 0) * 1.2)

    if sampling_percent == 0.0:
        print('No sampling found, returning call')
        return test_call['data']

    print("Finding number of sessions for anti-sample calculations...")
    explore_sessions = fetch_ga_data(service_acount_path, view_id, start_date, end_date, metrics = {'ga:sessions'}, dimensions = {'ga:date'})
    df = pd.DataFrame(explore_sessions['data'])
    df['date'] = pd.to_datetime(df['date'])
    df['sessions'] = pd.to_numeric(df['sessions'])
    df['cumulative'] = df['sessions'].cumsum()
    df['sample_bucket'] = chunkify_sessions(df)

    # Split to find new date ranges
    grouped_by_sample_bucket = df.groupby('sample_bucket')

    new_date_ranges = grouped_by_sample_bucket['date'].agg(['min', 'max', 'size']).rename(columns={'min': 'start_date', 'max': 'end_date', 'size': 'range_date'})

    print(f'Calculated [{len(new_date_ranges)}] batches are needed to download approx. [{row_count}] rows unsampled.')

    print(f'Found [{read_counts}] sampleReadCounts from a [{space_size}] samplingSpaceSize.')

    # Send to fetch
    did_it_work = True
    unsampled_list = []
    for index, rows in new_date_ranges.iterrows():
        print(f'Anti-sample call covering {rows["range_date"]} days: {rows["start_date"]}, {rows["end_date"]}')
        unsampled_batch = fetch_ga_data(service_acount_path, view_id, rows["start_date"], rows["end_date"], metrics, dimensions)

        read_counts2 = int(unsampled_batch['info']['samplesReadCounts'] or 0)
        space_size2 = int(unsampled_batch['info']['samplingSpaceSizes'] or 0)
        samplingPercent2 = read_counts2 / space_size2 if space_size2 else 0.0

        if samplingPercent2 != 0.0:
            print('Anti-sampling failed')
            did_it_work = False
        
        unsampled_list.append(unsampled_batch['data'])

    unsampled_df = pd.concat(unsampled_list)

    print(f'Finished unsampled data request, total rows [{unsampled_df.shape[0]}]')
    if did_it_work:
        print('Successfully avoided sampling')
    
    return unsampled_df

def chunkify_sessions(sessions_df, limit=250000):
    """Break down a request into unsampled chunks.

    Keyword arguments:
    sessions_df -- A pandas dataframe, ordered by date, with the number of sessions
    limit -- This is the upper bound for the number of sessions in one chunk (default 250000)

    Return:
    A list with the batch number of each date. This allows for usage in a mutate.
    """
    batch_size = 0
    batch_number = 1
    batch_numbers = [1] * sessions_df.shape[0]
    
    for x in list(range(sessions_df.shape[0])):
        sessions = sessions_df['sessions'][x]
        batch_size = batch_size + sessions

        if batch_size >= limit:
            batch_number = batch_number + 1
            batch_size = sessions
        
        batch_numbers[x] = batch_number
    
    return batch_numbers

def chunkify_metrics_fields(field_set, field_set_batch_size_limit):
    batch_set_list = []
    batch_set = []
    batch_size = 0
    field_number = 0
    total_fields_count = len(field_set)
    for field in field_set:
        field_number = field_number + 1
        batch_size = batch_size + 1
        batch_set.append(field)
        
        if field_number == total_fields_count:
            # Reached total field length. Adding current batch set to batch set list
            batch_set_list.append(batch_set)
        elif len(batch_set) >= field_set_batch_size_limit:
            # Reached field batch size limit. Adding current batch set to batch set list and resetting batch set and batch size counter
            batch_set_list.append(batch_set)
            batch_set = []
            batch_size = 0

    return batch_set_list

def ga_api_request_data(service_acount_path, view_id, start_date, end_date, metrics, dimensions, anti_sampling = False):
    print('Check if more than 10 metrics are needed and split into multiple calls then join')
    if len(metrics) <= 10:
        print('Less than 10 metrics requested. No need to split up request')
        if anti_sampling:
            final_df = anti_sample(service_acount_path, view_id, start_date, end_date, metrics, dimensions)
        else:
            final_df = fetch_ga_data(service_acount_path, view_id, start_date, end_date, metrics, dimensions)
    else:
        print('More than 10 metrics requested, splitting up requests...')
        metrics_batch_set_list = chunkify_metrics_fields(metrics, 10)
        print(f'Sending {len(metrics_batch_set_list)} requests and joining on the dimensions.')
        for metric_batch_set in metrics_batch_set_list:
            print(f'Starting batch [{metrics_batch_set_list.index(metric_batch_set)+1}] of [{len(metrics_batch_set_list)}] batches')
            if anti_sampling:
                batch_df = anti_sample(service_acount_path, view_id, start_date, end_date, metric_batch_set, dimensions)
            else:
                batch_df = fetch_ga_data(service_acount_path, view_id, start_date, end_date, metrics, dimensions)
            if metrics_batch_set_list.index(metric_batch_set) > 0:
                df_dimension_list = [dimension.replace("ga:", "") for dimension in dimensions]
                final_df = pd.merge(final_df, batch_df, how="left", on=df_dimension_list)
            else:
                final_df = batch_df
    
    print('Finished creating final dataframe.')
    return final_df