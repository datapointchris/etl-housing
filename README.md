
<h1 align="center">
  Apartment Rental ETL Pipeline
  <br>
</h1>

<h4 align="center">This program automatically scrapes, cleans, and stores apartment rental data daily.<br />
Using NLP to find description and amenity keywords that influence price.<br />
Using machine learning and visualization to estimate the price of a rental.</h4>

<p align="center">
	<a href="#description">Description</a> •
	<a href="#features">Features</a> •
	<a href="#future-features">Future Features</a> •
	<a href="#file-descriptions">File Descriptions</a> •
	<a href="#how-to-use">How To Use</a> •
	<a href="#requirements">Requirements</a> •
	<a href="#credits">Credits</a> •
	<a href="#license">License</a>
<br />
<br />
<img src='images/pipeline.jpg' height=400>
</p>


## Description

This project was created with the initial purpose of learning to scrape 'messy' data and clean it through a pipeline of functions automatically.  Some features may not be implemented perfectly, or be missing entirely.  Clean, functional, decoupled code is the main purpose of this project, along with learning how to implement traditional relational databases and NoSQL databases.


## Features

* Scrapes all of the listings for search term
* Cleans data for analysis
* Stores data in database
* Event logging
* NLP of descriptions and amenities
* Machine learning and visualization of price influencers.


## Future Features

* Load different formats into database
* MongoDB integration for articles
* Visualize Data
* Machine Learning algorithms to find key price predictors.
* Options Run on Command Line
* Web Interface with more options


## File Descriptions

`trulia_scrape.py` - this file can be run from the command line and will automatically scrape apartment data for the Austin area and save it to a CSV file in the `daily_scrape_files` folder.



## How To Use


```bash
# Clone this repository
$ git clone https://github.com/datapointchris/etl_housing

# Go into the repository
$ cd etl_housing

# Run the app
$ python scraper.py
```

Program will begin scraping Trulia for rentals.  Currently only Austin rentals have been tested.  Other cities and search terms will be available in future versions.

**NOTE**

Jupyter Notebooks are also included in the repo where you can run the program and change the `page_url` to scrape different cities.


## Requirements

You really should only need to install BeautifulSoup if you don't have it.  Everything else should be part of the standard library.

- Numpy
- Pandas
- Requests
- BeautifulSoup
- SQLite3


## Credits

- [Complete Data Analytics Solution Using ETL Pipeline in Python](https://medium.com/datadriveninvestor/complete-data-analytics-solution-using-etl-pipeline-in-python-edd6580de24b)
- Function framework based off [ScrapeHero](https://www.scrapehero.com/web-scraping-tutorials/)
- [Corey Schafer](https://www.youtube.com/channel/UCCezIgC97PvUuR4_gbFUs5g)


## License

[MIT](https://tldrlegal.com/license/mit-license)