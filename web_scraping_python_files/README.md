This folder contains the web scraping library and the Jupyter notebook file that was used to access the site data

call_scraper.ipynb -- jupyter notebook where web scraping function is called
scraper_lib.py -- library that contains all web scraping internal functions. Be sure to specify the correct directory to save the output files here and also to provide the library a set of proxies to sample (myprivateproxy.net is a great source for ~$20 month for ~10 private proxies)

the central **scraper_lib.get_all_tourn_data(year)** function in the jupyter notebook can be called multiple times even if a statistics file corresponding to the year in question already exists. Upon successive calls, the functions updates the existing statistics file to include matches that have been played since the file was assembled. Links in the error file for that year and retried for match data as well, and if succesful, these statistics are added to the main statistics file and the associated url is remvoed from the error list.

Lastly the jupyter notebook also performs some light cleaning of the data to search for situations in which first serve makes have been mislabeled and also to cast out any entries with nonsensical zero values.
