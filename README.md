
<h1 align="center">
  Apartment Rental ETL Pipeline
  <br>
</h1>

<h4 align="center">Scrapes, cleans, and stores apartment rental data.</h4>

<p align="center">
	<a href="#description">Description</a> •
	<a href="#features">Features</a> •
	<a href="#future-features">Future Features</a> •
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


## Future Features

* Load different formats into database
* MongoDB integration
* Visualize Data
* Machine Learning algorithms to find key price predictors.
* Options Run on Command Line
* Web Interface with more options


## How To Use

```bash
# Clone this repository
$ git clone https://github.com/datapointchris/etl_housing

# Go into the repository
$ cd etl_housing

# Install requirements
$ pip install requirements.txt

# Run the app
$ python scraper.py
```

Program will begin scraping Trulia for rentals.  Currently only Austin rentals have been tested.  Other cities and search terms will be available in future versions.

**NOTE**

Jupyter Notebooks are also included in the repo where you can run the program and change the `page_url` to scrape different cities.


## Requirements

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