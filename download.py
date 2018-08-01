###################################################
###################################################
###                                             ###
###   Downloads GDELT saved weblinks            ###
###   Currently downloads only 2017 websites    ###
###   Adapted for GDELT mentions.CSV            ###
###   By Lee Boon Keong                         ###
###                                             ###
###################################################
###################################################

import wget         # For downloading files from websites.
import os           # For listing directory files.
import os.path      # To check if file exists.
import linecache    # To read specific lines in Text files.
import re           # RegEx! - Regular Expression.
import zipfile      # Fpr unzipping files.

# Set global variables.
Master_Link = 'http://data.gdeltproject.org/gdeltv2/masterfilelist.txt'
Master_Name = Master_Link.split('/')[-1]            # Get the name of the file.
Path_CSV = "./csv/"

# Function to download the Master File
def download_file(link, fname="File_Download", path="./" , override=False):
    #Set the propert path to file
    full_fname = path + fname

    # Check to see if Path directory exist, if any. If doesn't exist, make it.
    if not os.path.exists(path):
        os.mkdir(path)
    
    if override == True:
        # Remove last old file. (i.e. "update" to latest file)
        if os.path.isfile(extr_fname(full_fname)):
            os.remove(extr_fname(full_fname))
        
        print ("Downloading... {}".format(fname))
        wget.download(link, full_fname)
        print ("Downloaded! ", fname)
    else:
        if os.path.isfile(extr_fname(full_fname)):
            print("File exists: ", extr_fname(full_fname))
        else:
            print ("Downloading... {}".format(fname))
            wget.download(link, full_fname)
            print ("\nDownloaded! ", fname)

# Function to count number of lines in text file.
def txt_len(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

# Reads the MasterFileList and extracts only the links for "*mentions.CSV" files of stated year.
def read_txt(fname, incre = 2, year = 2017):
    
    link_list = []
    runfor = int(txt_len(fname)/3)
    
    for i in range(runfor):
        line_lvl = (i-1)*3 + incre
        read_line = linecache.getline(fname, line_lvl)          # Reads the 'i*lvl'-th line of the text file
        try:
            if year == 2018: 
                extr_link = re.search(r"(http://data.gdeltproject.org/gdeltv2/2018(.+).zip)", read_line)
            elif year == 2017:
                extr_link = re.search(r"(http://data.gdeltproject.org/gdeltv2/2017(.+).zip)", read_line)
            elif year == 2016:
                extr_link = re.search(r"(http://data.gdeltproject.org/gdeltv2/2016(.+).zip)", read_line)
            elif year == 2015:
                extr_link = re.search(r"(http://data.gdeltproject.org/gdeltv2/2015(.+).zip)", read_line)
            else:
                pass

            link_only = extr_link.group()
            link_list.append(link_only)
        except:
            pass

    return link_list

# Gets filename from web links.
def get_fname(link):
    fname = link.split('/')[-1]
    return fname

# Extracts filename and path of any '.CSV' file only.
def extr_fname(fname):
    try:
        extr = re.search(r"(.+).CSV", fname)
        return extr.group()
    except:
        return fname

# Unzips file in states path and deletes zip file.
def unzip(zip_name, path = "./"):
    try:
        zip_ref = zipfile.ZipFile(path + zip_name, 'r')
        zip_ref.extractall(path)
        zip_ref.close()
        print ("\nUnzip DONE!")
        os.remove(path + zip_name) # Remove file after unzipping.
    except:
        print("Unzip error. CSV file likely already exists. Continue...")

# Main function to process all downloading and unzipping of all links in MasterList.
def download_file_multi(list, path="./"):
    try:
        i = 0
        for link in list:
            file_name = get_fname(link)
            download_file(link, file_name, path=path, override=False)
            unzip(file_name, path=path)
            i += 1
            print("Downloaded {} out of {}".format(i, len(list)))
    except: 
        print("Error in multi downloading...")
        pass

def main():
    # Set 'override' to 'True' to update MasterFileList, else 'False' to keep:
    download_file(Master_Link, Master_Name, path=Path_CSV, override = True)    
    
    # Extract links to files for the year 2017 and save into a list.
    csv_list = read_txt(Path_CSV + Master_Name, incre = 2, year = 2017)
    print(len(csv_list))
    download_file_multi(list=csv_list, path=Path_CSV)

if  __name__ == '__main__':
    main()

    