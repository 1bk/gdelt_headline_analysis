###################################################
###################################################
###                                             ###
###   Processes CSV files into a Database       ###
###   Run only after 'download.py'              ###
###   Adapted for GDELT mentions.CSV            ###
###   By Lee Boon Keong                         ###
###                                             ###
###################################################
###################################################

import pandas as pd             # To handle dataframes
import numpy as np              # To help handle dataframes
from dbhelper import DBHelper   # For database. The "dbhelper.py" file should be in the same dir.
import os, re                   # To help access files and directories.
import pickle                   # To save objects (variables) for future use.
import sys                      # For progress bar, and exiting script.

# Set global variables and/or functions
db = DBHelper()             # Abbreviate the database helper for ease of use.
Path_CSV = "./csv/"         # Sets the path to "csv" folder. Remember to include "/" at the end.
proc_stat = {}              # A dictionary of the list of files and its process status. To be Pickled! 

# Function to build a dictionary with 'keys' the CSV file names in the Path_CSV directory, 
# and assigns "0" as its value to indicate the unprocess status. 
def build_csv_dict(pickle = "ProcessStatus.pickle"):
    
    # Function that gets only the CSV filenames and save them into an array.
    def list_all(ftype = "CSV", path="./"):
        onlyfiles = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
        r = re.compile(".*\." + ftype)
        return list(filter(r.match, onlyfiles)) 

    if os.path.isfile(pickle):
        csv_dict = unpickle_it(pickle)
    else:
        csv_list = list_all("CSV", Path_CSV)    # Build a list of the CSV files in Path_CSV.
        csv_dict = {x:0 for x in csv_list}      # Set up the dictionary to track process status.
    
    return csv_dict                             # Returns a dictionary.

# Function to process all the CSV in the "/.csv/" directory into the SQLite database.
def process_all_csv(csv_dict, path=Path_CSV):
    try:
        i = 0
        n = len(csv_dict) 
        for CSV in csv_dict:
            progress(i, n, "Processing now... Stand-by...")
            if csv_dict[CSV] == 0:
                db.add_csv(path + CSV)
                csv_dict[CSV] = 1           # Set key value to "1" to indicate processed.
                if i % 1000 == 0:
                    db.remove_duplicates()
                elif i == n:
                    db.remove_duplicates()
                else: pass
            else: pass
            i += 1
        print("Process {}/{} CSV files.".format(i, n))
        return csv_dict                     # Returns a dictionary of process status. Save this!
    except KeyboardInterrupt:
        pickle_it(csv_dict, "ProcessStatus.pickle")
        print("Process Status pickled.")
        sys.exit("\nKeyboard interruption while processing CSV. Exiting script now.")
    except:
        print("Other error occured while processing.")
    finally:
        pickle_it(csv_dict, "just_in_case.pickle")
        print("Just in case pickle pickled. :)")

# Function to save an object for future use when re-running script.
def pickle_it(obj, fname="saved.pickle"):
    obj_file = open(fname,'wb')             # Open the file for writing.
    pickle.dump(obj, obj_file)              # Writes the object a to the file named as per 'fname'.
    obj_file.close()                        # Close the obj_file.

# Function to load an object from previous use when script was last runned.
def unpickle_it(fname):
    obj_file = open(fname,'rb')             # Open the file for reading.    
    obj = pickle.load(obj_file)             # Load the object from the file and return it.
    obj_file.close()
    return obj

# Progress Bar
# Source: https://gist.github.com/vladignatyev/06860ec2040cb497f0f3
def progress(count, total, status=''):
    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))
    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)
    sys.stdout.write('[%s] %s%s ... %s\r' % (bar, percents, '%', status))
    sys.stdout.flush() 

def main():
    db.setup()                                      # This sets up the Database, if non exists.
    print("Done! - Database set up.")

    proc_stat = build_csv_dict()
    print("Done! - CSV Dictionary established/unpickled.")
    
    proc_stat = process_all_csv(proc_stat)          # Start Processing all the CSV in "./csv/" directory.
    print("Done! - Processing all CSVs.")           # Then updates the Process Status dictionary.

    pickle_it(proc_stat, "ProcessStatus.pickle")    # Save the processing status.
    print("Done! - DataFrame Pickled.")
    
    print("Now please proceed run 'main()' in 'extract.py' file.\n")

if  __name__ == '__main__':
    main()