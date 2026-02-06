# GitHub Copilot Instructions

## Project Overview

This project builds a comprehensive environmental monitoring database for the 
Netherlands, integrating water quality, chemical, and ecological data from 
various Dutch authorities.

Key aspect of this project is an architecture where the envdb API is the 
generic abstract middle which provides support to other external modules to
create imports and exports. The envdb must be ignorant of these other modules.

The underlying db is intended be maintained by Github Actions.

## Copilot task

Your task is to assist with the development of the envdb API and its related components, 
not maintain the actual db.

## Directory Overview

* code – All matured python code. 
  * envdb – API for the database. In this project, we have a fully generic db schema.
    The API is generic and reflective so it can be used by other modules that will
    map their data to the generic schema and modules that can map the generic schema
    to specific needs, for example geojson files.
  * viewer – A HTML/JS based viewer for exploring the data.
* water – Rijkswaterstaat data processing. Used to download data from RWS and normalize
  it for the envdb API.
  * aquo – Processing of the Aquo domain, which is the controlled vocabulary for the Dutch water sector.
  * import – Scripts to import RWS data into the database. This needs work
    because we need the envdb to be stabilized.
  * csvs – All CSV files, these are not stored on github
* scripts – All scripts, will be part of the PATH so convenient to use. Finalized
  scripts will be moved here. Never add anything here except on request.
  * rws – download RWS data via their API as CSV files
  * aquo – download and Aquo domain data as CSV files
* copilot – Working directory for copilot. All code files and test files that are not yet 
  mature enough to be moved to a final destination remain here. This directory is not stored on
  github.


## Secondary Goals

The intention is that this envdb will be also useful for other countries (hence, English) 
and for other geometric sampled data.

## envdb schema API

basic model is for now:

* authority – the authority responsible for the data (e.g. RWS, Provincie, Waterschap)
* region – a geographic region (e.g. a province, a water body)
* station – a monitoring station within a region
* sample – a specific sample taken at a station at a specific time, the type is defined via a Parameter (a class). It uses a float column or a json colum.
* pubchem – a table for chemical compound data from PubChem

Key issue is that the DB will have several data sources implemented in the library and
not the db.

## External Dependencies

- **RWS WKP API** - `https://wkp.rws.nl/api/v1/data-downloads/download`
- **Aquo.nl** - Dutch water sector controlled vocabulary. The data is crawled from the pages
  starting at https://www.aquo.nl/index.php/Categorie:Actueel
- **PubChem API** - Chemical compound enrichment (rate-limited to 5 req/sec)
  at https://pubchem.ncbi.nlm.nih.gov/rest/pug/disambiguate/name/JSON?name=enrofloxacin

