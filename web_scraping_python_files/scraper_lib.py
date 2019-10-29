
import time
import random 
from bs4 import BeautifulSoup as soup
import requests
import csv
import os
from multiprocessing import Pool


#set to directory where files are to be saved
os.chdir(r'C:\Users\Anjana Puri\Documents\Python\TennisProject\JosephSackmann Data\tennis_atp-master\tennis_atp-master\ATP_match_data')

#global variables for rotating proxy/ip_address
ip_track=0
user_agent_track=0

#load proxy addresses into memory
proxy_file=open('proxies.txt','r')
proxies=[]
for line in proxy_file:
    proxies.append(line[0:line.find('p')-1])

#user agent master list
user_agent_list=[
   #Chrome
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (Windows NT 5.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
    #Firefox
    'Mozilla/4.0 (compatible; MSIE 9.0; Windows NT 6.1)',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 6.2; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)',
    'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)'
]

"""
function opens a given url with a proxy from our proxy list and returns its contents
    input: url --> web address of page to be selected


"""
def open_url(url):
    global proxies
    global ip_track
    global user_agent_list
    global user_agent_track
    
    #make sure successive calls come from different proxy/agent combination
    proxy=proxies[ip_track%len(proxies)]
    agent=user_agent_list[user_agent_track%len(user_agent_list)]
    url_info=requests.get(url,headers={'User Agent':agent},proxies={'https':'http://ppuri:vd6BfzMr@'+proxy})
    return url_info.content


"""
function will be used to append data to a csv file. Will be used when acquring match data
or bad urls

    input: data ---> information to be appended to files (single row)
           file ---> filename of csv file
    
"""

def append_to_csv(data,file):  
    if data==None:
        return None
    else:
        with open(file,'a',newline='') as csvFile:
            writer = csv.writer(csvFile)
            writer.writerow(data)
    return None


"""
looks at all the match data in a given match data csv file and checks whether the 
accumulted bad_url links have been fetched or not. If so, these bad url are removed from
the error csv file

    input: data_file--->filename of match data file. One of these columns contains all successfully
                        fetched matched links
            url_file---->filename of file containing all urls that were unable to be fetched when
                         data_file was created.
    performed operation: function checks whether some of the links in bad_urls have in fact actually now
                        been fetched and removes them if so

"""

def update_bad_urls(data_file,url_file):
    fetched_urls=get_fetched_links(data_file)
    unfetched_urls=get_bad_urls(url_file)
    remaining_bad_links=[i for i in unfetched_urls if i not in fetched_urls]
    os.remove(url_file)
    append_to_csv(['Unfetched URLs'],url_file)
    
    for link in remaining_bad_links:
        append_to_csv([link],url_file)
    return None
    

"""
gets all match links that have been successfully fetched and parsed for match data

    input: data_file--->filename of match data file
    output: fetched_links---->all match links whose match data was stored in data_file

"""


def get_fetched_links(data_file):
    
    data_header=['year','match link','p1 id (winner)','p2 id (loser)','p1 name', 'p2 name',
                 'p1 aces','p1 double faults','p1 FSP','p1 total first serves made','p1 total first serves hit',
                 'p1 total first serve points won','p1 total first serve points','p1 total second serve point won',
                 'p1 total second serve points', 'p2 aces','p2 double faults','p2 FSP','p2 total first serves made',
                 'p2 total first serves hit','p2 total first serve points won','p2 total first serve points','p2 total second serve point won',
                 'p2 total second serve points']
    
    fetched_links=[]
    if os.path.exists(data_file):
        f=open(data_file,'r')
        line=0
        for ele in f:
            if line==0:
                line+=1
                continue
            else:
                fetched_links.append(ele.split(',')[1])
                
        f.close()
    else:
        append_to_csv(data_header,data_file)
        
    return fetched_links


"""
gets bad_urls that were stored in the error file for a particular year

    input: bad_link_url--->filename of match data file
    output: bad_urls---->all match links that were unable to be fetched in most recent
                         fetching of match data for a particular year
"""


def get_bad_urls(bad_link_url):
    bad_urls=[]
    if os.path.exists(bad_link_url):
        f=open(bad_link_url,'r')
        line=0
        for ele in f:
            if line==0:
                line+=1
                continue
            else:
                bad_urls.append(ele.translate(str.maketrans("", "",'\n')))
                
        f.close()
    else:
        append_to_csv(['Unfetched URLs'],bad_link_url)
    return bad_urls
    
"""
gets links that directs to the scores/results page for all tournaments in a given year

    input: year--->year which links are needed
    output: tourn_link_list----> links to all tournament results pages in a given year
"""



def tourn_link_list(year):
    delay=random.random()*3+3
    time.sleep(delay)
    year_lxml=soup(open_url("https://www.atptour.com/en/scores/results-archive?year="+str(year)),"lxml")
    
    link_list=[]
    for link in year_lxml.find_all('a'):
        link_list.append(link.get('href'))
    link_list=[i for i in link_list if type(i)==str]
    tourn_link_list=['https://www.atptour.com/'+i for i in link_list if (i[-12:]==str(year)+'/results') and (not 'doubles' in i)]
    return tourn_link_list


"""
takes a link to a tournament results page and returns all links to match stats on that page

    input: tourn_url--->link to tournament results page
    output: match_links---->all links to match stats on that page

"""


def get_matches_in_tournament(tourn_url):
    match_links=[]
    #access tournament results page
    tourn_lxml=soup(open_url(tourn_url),'lxml')
    
    #parse page contents for links
    for link in tourn_lxml.find_all('a'):
        temp_link=link.get('href')
        
        #make sure all acquired links are in fact strings
        if type(temp_link)==str:
            
            #make sure they don't contain the '/402/' characters link since those are error flags
            if ('match-stats' in temp_link) and (not '0/402/' in temp_link):
                match_links.append('https://www.atptour.com/'+temp_link)    
    return match_links
 


"""
looks at all the links to match results in a given year (typically numbering ~3000-4000)
and determines which one have not been properly fetched or generated errors in most recent fetching

    input: year---> year for which unfetched links would like to be generated
    output: links_to_query----> links that have not yet been properly parsed for match data
"""

def get_unfetched_matches(year):
    
    print("Year: {}".format(year))
    
    #generate file names that will be used for the match_data and the bad_url files
    file_save='match_data_'+str(year)+'.csv'
    bad_link_save='errors_'+str(year)+'.csv'
    
    #look at all links that have already been fetched for this year
    existing_good_links=get_fetched_links(file_save)
    #looks for any urls which generated errors in most recent fetching
    existing_bad_links=get_bad_urls(bad_link_save)
       
    #now generate a list of all match links for a given year
    all_match_links=[]
    tour_links=tourn_link_list(year)
    pool=Pool(8)
    print('Fetching match links...')
    for link_batch in pool.map(get_matches_in_tournament,tour_links):
        for links in link_batch:
            all_match_links.append(links)
    pool.terminate()
    pool.join()
    
    #check which links have not already been stored in the match_data_file and return the list
    links_to_query=list(dict.fromkeys([i for i in (all_match_links+existing_bad_links) if i not in existing_good_links]))
    print('{} links fetched'.format(len(links_to_query)))
    
    return links_to_query



"""
takes contents of match_statistics url and parses it for player stats

    input: soup ---> Beatiful soup lxml parsing of match result page
    output: player_resutls---> a series of match statistics including player names, ids, aces, double faults, and first/second
            serve winnning/make percentages (see header list in above function for details)
    
"""
def convert_to_stats(soup,player):
    if player=='winner':
        page_data=soup.find_all('td',{"class":"match-stats-number-left"})
    else:
        page_data=soup.find_all('td',{"class":"match-stats-number-right"})
        
    nums=[]
    index_list=[1,2,3,4,5,7,8,10,11]
    for stat in page_data:
        num=stat.text.translate(str.maketrans("", "",'\n\r '))
        
        if not '(' in num:
            nums.append(int(num))
        else:
            nums.append(int(num[:num.find('%')]))
            nums.append(int(num[num.find('(')+1:num.find('/')]))
            nums.append(int(num[num.find('/')+1:num.find(')')]))
    player_results=list(map(nums.__getitem__, index_list))        
    return player_results


"""
takes contents of match_statistics url and parses it for player names

    input: soup ---> Beatiful soup lxml parsing of match result page
    output: player_names---->list of first and last names for each player in match (winner comes first)
    
"""


def player_names(soup):
    first_names=soup.find_all('span',{'class':"first-name"})
    last_names=soup.find_all('span',{'class':"last-name"})
    
    #list of first names with white spaces included
    first_names_ws=[i.text.translate(str.maketrans("", "",'\n\r%')).split(" ") for i in first_names]
    #now take care of white spaces
    first_names=[" ".join([word for word in name if word!=""]) for name in first_names_ws]
    
    #list of last names with white spaces included
    last_names_ws=[i.text.translate(str.maketrans("", "",'\n\r%')).split(" ") for i in last_names]
    #now take care of white spaces
    last_names=[" ".join([word for word in name if word!=""]) for name in last_names_ws]
    
    return [first_names[i]+" "+last_names[i] for i in range(2)]


"""
takes contents of match_statistics url and parses it for player ids

    input: soup ---> Beatiful soup lxml parsing of match result page
    output: player_ids---->list of player_ids for each player in match (winner comes first)
    
"""

def get_ids(x):
    ids=[]
    for link in x.find_all('a'):
        temp_link=link.get('href')
        if type(temp_link)==str and temp_link[-8:]=='overview':
            player_id=temp_link[12:]
            start_slash=player_id.find('/')+1
            stop_slash=player_id[start_slash:].find('/')+start_slash
            ids.append(player_id[start_slash:stop_slash])
    player_ids=list(dict.fromkeys(ids))    
    return player_ids

"""
takes a url linking to a statistics page for a match and extracts the results

    input: match_url ---> link to given stats page for a map
    output: all_stats, error_track
            all_stats--->match information (link, player name/ids, player stats) if no error occurs
                        if error occurs, all_stats is a one element list containing the match_url
            error_track---->1 if error occurs, 0 if not
            
    
"""


def match_entry(match_url):
    #add a random delay to every fetching to avoid being id'ed as a robot
    delay=random.random()*3+3
    time.sleep(delay)
    hit=0
    
    #try to connect to url up to 20 times before giving up
    while hit==0:
        try:
            hit+=1
            html = open_url(match_url)
        except:
            print('Trouble connecting. Trying again...')
            delay=random.random()*1+1
            time.sleep(delay)
            if hit==20:
                raise Exception('Trouble connecting to site, shutting down for now....')
        
    html_soup=soup(html,"lxml")
    error_track=0
    
    try:
        #try to get stats. If unsuccessfull, return error_track=1
        winner_stats=convert_to_stats(html_soup,'winner')
        loser_stats=convert_to_stats(html_soup,'loser')
        player_ids=get_ids(html_soup)
        names=player_names(html_soup)
        all_stats=[match_url]+player_ids+names+winner_stats+loser_stats

    except:
        print('Trouble parsing. Moving on...')
        all_stats=[match_url]
        error_track=1
        
    return all_stats, error_track


"""
looks at all matches in a given year and stores match data in a csv file and also urls that generated errors
in a separate file so they may be returned too later. If some match data has already been retrieved for a given year, then only 
new match data will be added to the resultant match_data file

    input: year---> year for which match data would like to be generated
    
"""

def get_all_tourn_data(year):
    
    #assign filenames to match_data and error files
    file_save='match_data_'+str(year)+'.csv'
    bad_link_save='errors_'+str(year)+'.csv'
    
    #determine which match links have yet to be successfully parsed for this year
    links_to_query=get_unfetched_matches(year)
    
    

    #tracking variables
    error_track=0
    success_track=0
    local_error_track=0
    local_success_track=0
    loop_track=0

    pool = Pool(8)  
    #separate the links into batches that can be fed into our multiprocessor so we can get real-time feedback on progress
    link_batches=[links_to_query[i:i + 50] for i in range(0, len(links_to_query), 50)]
    
    for batch in link_batches:
        
        for match_data,error in pool.map(match_entry,batch):
            
            #keep track of the number of times the loop has been executed
            loop_track+=1
            
            #if error has been generated, add match_url (output of match_data) to bad_url error file
            if error==1:
                local_error_track+=1
                print("Error has occured on {}, skipping match...".format(match_data[0]))
                append_to_csv(match_data,bad_link_save)
            else:
            #otherwise add match data to match_data csv file
                local_success_track+=1
                append_to_csv([year]+match_data,file_save)
        
        #every 50 matches output some results
        print("Percent Complete: {}".format(loop_track/len(links_to_query)))
        print("Number of successes in last 50 matches: {}".format(local_success_track))
        print("Number of errors in last 50 matches: {}".format(local_error_track))
            
        error_track+=local_error_track
        success_track+=local_success_track
        local_error_track=0
        local_success_track=0
            
    print("Total successful pulls: {}".format(success_track+local_success_track))
    print("Total errors: {}".format(error_track+local_error_track))
    pool.terminate()
    pool.join()
    
    #update the bad_url file to remove any previous links that generated errors if they have now 
    #been successfully parsed
    
    update_bad_urls(file_save,bad_link_save)
            
                
    return None





                                                           