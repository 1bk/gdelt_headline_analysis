###################################################
###################################################
###                                             ###
###   Extracts a Dataframe from SQL Database    ###
###   Run only after 'process.py'               ###
###   Adapted for GDELT mentions.CSV            ###
###   By Lee Boon Keong                         ###
###                                             ###
###################################################
###################################################

from dbhelper import DBHelper   # For database. The "dbhelper.py" file should be in the same dir.
from process import pickle_it, unpickle_it # To save objects (variables) for future use.
import pandas as pd             # To handle dataframes.
import numpy as np              # To help handle dataframes.
import requests as re           # To handle website's HTML.
from bs4 import BeautifulSoup as bs # For extracting <h1> from HTML.
import sys                      # For progress bar, and exiting script.

# Set global variables and/or functions
db = DBHelper()                 # Abbreviate the database helper for ease of use.
# Feel free to change these values:
sz_samp = 1500
sz_head = 1000
website1 = "bbc.co.uk"
website2 = "indiatimes.com"
df_extr = "DataFrame_Extract.pickle"
df_samp = "DataFrame_Sample.pickle"
df_head = "DataFrame_Headlined.pickle"


# Extracts 2 websites' links into a dataframe and adds an empty Headlines column.
def build_df_extract(site1, site2, pickle="DataFrame.pickle"):
    print("Extracting DataFrame from SQLite Database. Please stand-by.")
    df_list = list(db.get_specific(site1, site2))
    df = pd.DataFrame(np.array(df_list))
    df.columns = ['Website', 'Link']
    df["Headline"] = ''
    pickle_it(df, pickle)
    print(df.sample(5))
    print("\nDone! - DataFrame completed. Pickled  \'", pickle,"\'")
    return df

def build_df_sample(df, sizeofeach=1000):
    site_list = list(df.Website.unique())
    print("Site to process:", site_list)

    df_r = pd.DataFrame()
    for site in site_list:
        df_t = df[df["Website"] == site].sample(sizeofeach).reset_index(drop=True)
        df_r = pd.concat([df_r, df_t], ignore_index=True) 

    return df_r

def get_topic(url):
    r = re.get(url)
    content = r.content
    soup = bs(content, "html.parser")
    heading = soup.find_all('h1')
    return str.strip(heading[0].text)

def build_df_headlines(df, pickle="DataFrame_Headlined.pickle"):
    try:
        for i in range(len(df)):    
            try:                    
                if (df["Headline"][i] == ''):
                    headline = get_topic(df["Link"][i]) 
                    print (i, "/", len(df), ": ",headline)
                    df["Headline"][i] = headline
                else:
                    print ('Row {} is not empty.\n'.format(i))
                    pass
            except Exception as e:
                print("Error getting headline, inserting '' and continue loop. Error: ", e)
                pass
        # Drop duplicate & empty headlines 
        # Source: https://stackoverflow.com/questions/23667369/drop-all-duplicate-rows-in-python-pandas
        df = df[df["Headline"] != ''].drop_duplicates(subset=["Headline"])
        pickle_it(df, pickle)
        return df
    except KeyboardInterrupt:
        df = df[df["Headline"] != ''].drop_duplicates(subset=["Headline"])
        pickle_it(df, pickle)
        sys.exit("\nKeyboard interruption while building headlines. Exiting script now.")
    except Exception as e:
        print ("ERROR: ", e)
        pass
        

# Returns a lists of dictionary
def build_site_dict(df, sizeofeach=1000):
    site_list = list(df.Website.unique())
    i = 0
    for site in site_list:  
        # Extract base on length of values of headline.
        # Source: https://stackoverflow.com/questions/46429033/how-do-i-count-the-total-number-of-words-in-a-pandas-dataframe-cell-and-add-thos
        # Source: https://stackoverflow.com/questions/45089650/filter-dataframe-rows-based-on-length-of-column-values
        df_site = df[(df["Website"] == site) & (df["Headline"].apply(lambda x: len(str(x).split(' '))) > 2)].sample(sizeofeach).reset_index(drop=True)
        
        # Creates a list of dictionary for each headline.
        headlines = [{"title": x} for x in df_site["Headline"]]
        pName = "Website" + str(i+1) + "v3.pickle"
        pickle_it(headlines, pName)
        i += 1

        print ("\nDictionary for {} built and saved as \"{}\".".format(site, pName))

    print("Done! Site dictionaries built and pickled in directory for", site_list)

def main():    
    # Step 1: Build that dataframe from dataset.
    df_1 = build_df_extract(website1, website2, df_extr)
    
    ### To handle if size of sample bigger than population
    global sz_samp
    global sz_head
    if len(df_1) < sz_samp:
        sz_samp = int(round(len(df_1)/4,-1))
        sz_head = int(round(sz_samp*0.75,-1))
        print("Size of df: {}; Size to sample: {}; Size for dict.: {}".\
        format(len(df_1),sz_samp, sz_head))
    ###
    
    # Step 2: Randomly extracts 2,000 links for each website into a new dataframe.
    # df_1 = unpickle_it(df_extr)
    df_2 = build_df_sample(df_1, sz_samp)
    # print(df_2.sample(10))

    # Step 3: 
    df_3 = build_df_headlines(df_2)
    
    # Step 4:
    # df_3 = unpickle_it(df_head)
    build_site_dict(df_3, sz_head)
    

if  __name__ == '__main__':
    main()