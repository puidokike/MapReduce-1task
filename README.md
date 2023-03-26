# Simplified MapReduce framework
This code demonstrates a simplified map-reduce framework written in Python and using Python's multiprocessing module. It takes a CSV file (which contains column 'date'), splits it into data chunks, processes (counts how many clicks there were for each date) the dataset in parallel and combines results into a final output CSV file. 

### Instructions
1. Ensure that Python 3.x is installed on your machine.
2. First run main.py file to autocreate directories.
3. Place the CSV file you want to process in the autocreated 'data/clicks' directory.
4. Modify the start_file and num_of_chunks variables in the __if __name__ == '__main__':__ block to specify the starting file and number of chunks to process.
5. The output CSV file will be located in the autocreated 'data/total_clicks' directory.

*Note: This code assumes that the input CSV file has a header row with the following column name: 'date'. If your input file has different column name, please rename it to 'date'.*
