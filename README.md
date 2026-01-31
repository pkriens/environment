# Environment

This repository is intended to be used to hold scripts and information 
related to the environmental status of the netherlands. 

The final goal might be to create a public database with environmental data
that can be used for analysis and reporting.

This is mostly initiated by the extremly obfuscated world of environmental
rules and data.

## rws command 

The `rws` command is a download module for the Dutch Rijkswaterstaat (RWS)
water data portal. It allows downloading datasets related to water quality,
ecological status, and other water-related measurements.

RWS allows downloading data via predefined subjects, each representing a specific
dataset or theme and a year via the page:

    https://wkp.rws.nl/downloadmodule

This page provides access to all information but there is no way to download
data for all years or all water authorities in one go. The `rws` command allows
downloading multiple datasets in an automated way.

Use `rws help` to see the available options.