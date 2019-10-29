# ATP_web_scraper
A web scraper for fetching match statistics from all matches stored on the ATP World Tour website

The scraper compiles a list of links that point to all match statistics pages for matches in a given year. It then parses each link, using multiprocess threading, for match statistics related to the player names, player ids, and player serving statistics. The ATP site pages were accessed from a set of 11 rotating proxies (private proxies purchased from myprivatepoxy.net) and rotating user agents to avoid being identified as robot. Each match pull was preceded by a 3-6 second time delay to avoid overburdening the site.

A minimal set of statistics was chosen to be fetched to meet the needs of the other ATP projects on my GitHub, but the parser can easily be modified to fetch more match information. For each match from 1991-2019, the following information was extracted:

year match was played </br>
match url </br>
player 1 id (4 digit code generated for each player in the ATP website) </br>
player 2 id (4 digit code generated for each player in the ATP website) </br>
player 1 name (winner) </br>
player 2 name (loser) </br>
player 1 aces </br>
player 1 double faults </br>
player 1 first serve make percentage </br>
player 1 total first serves made </br>
player 1 total first serves hit </br>
player 1 total first serve points won </br>
player 1 total first serve points played </br>
player 1 second serve points won </br>
player 1 second serves points played </br>
player 2 aces </br>
player 2 double faults </br>
player 2 first serve make percentage </br>
player 2 total first serves made </br>
player 2 total first serves hit </br>
player 2 total first serve points won </br>
player 2 total first serve points played </br>
player 2 second serve points won </br>
player 2 second serves points played </br>


Match statistics from each year are stored in separate CSV files and links that were unable to be parsed are also stored in CSV files for reference. 

The central match-statistics fetching function is called from a Jupyter notebook that imports an custom iPython web_scraping library, to enable the multiprocessing to occur smoothly in Windows. 



