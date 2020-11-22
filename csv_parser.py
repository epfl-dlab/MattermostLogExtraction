import csv

def write_csv(data, filename):
    """Write the data to the filename file in csv format.

    Parameters
    ----------
    data : The data to write in csv format
    filename : The name of the file
    """
    with open(filename, mode='w') as wfile:
        file_writer = csv.writer(wfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)

        print("Start writing data into {0} file".format(filename))
        for row in data:
            file_writer.writerow(row)
