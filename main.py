import csv
import os
from multiprocessing import Pool


def map_reduce(file, chunks_num):
    # 1st step - reading the csv file using "csv" module
    with open(file, 'r') as csvfile:
        reader_csv = csv.DictReader(csvfile)
        dataset = []
        for row in reader_csv:
            dataset.append(row)

    # 2nd step - splitting data into chunks
    rows_total = len(dataset)
    rows_remain = rows_total % chunks_num
    chunk_size = (rows_total - rows_remain) // chunks_num
    if rows_remain > 0:
        chunk_size += 1

    chunks = []
    for i in range(0, len(dataset), chunk_size):
        chunk = dataset[i:i + chunk_size]
        chunks.append(chunk)

    # 3rd step - using "multiprocessing" module to process mapped data in parallel
    with Pool() as pool:
        results = pool.map(mapping, chunks)

    combined_result = {}
    for res in results:
        for date_value, click in res.items():
            if date_value in combined_result:
                combined_result[date_value] += click
            else:
                combined_result[date_value] = click

    # 4th step - reducing data
    res_dir_path = 'data/total_clicks'
    res_file_path = os.path.join(res_dir_path, 'total-clicks.csv')
    if not os.path.exists(res_dir_path):
        os.makedirs(res_dir_path)
    with open(res_file_path, 'w', newline='') as csvfile:
        col_names = ['date', 'clicks']
        writer_csv = csv.DictWriter(csvfile, fieldnames=col_names)
        writer_csv.writeheader()
        for date, clicks in combined_result.items():
            writer_csv.writerow({'date': date, 'clicks': clicks})


# Mapper used in 3rd step
def mapping(chunk):
    mapped = {}
    for row in chunk:
        date = row['date']
        if date in mapped:
            mapped[date] += 1
        else:
            mapped[date] = 1
    return mapped


if __name__ == '__main__':
    # Fill in start_file name and chunks_num manually:
    start_file = 'part-001.csv'
    num_of_chunks = 5

    dir_path = 'data/clicks'
    file_path = os.path.join(dir_path, start_file)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    map_reduce(file_path, num_of_chunks)
