# AI-aided company search with Weaviate

This is a small demo of how easily - and without any AI/machine learning
knowledge - [weaviate](https://github.com/semi-technologies/weaviate) can be
used to build a very powerful concept search.

## Video Demo of this repo

Coming soon...

## What can this do

Although the list contains very little information a search about concepts
becomes possible. For example a search for "Computers" lists typical IT
companies - even though none of them mention the word computer anywhere in the
data.

This works through the machine-learning trained Contextionary which powers
Weaviate.

## Source of companies

The list in list.txt is copy/pasted from [the S&P 500 Wikipedia
page](https://en.wikipedia.org/wiki/List_of_S%26P_500_companies).

## Source of financial data

Downloaded from [here](https://datahub.io/core/s-and-p-500-companies-financials).

> Note that while no credit is formally required a link back or credit to
> [Rufus Pollock](http://dev.rufuspollock.org/) and the [Open Knowledge
> Foundation](http://okfn.org/) is much appreciated.

## Content of this repo

A very simple import script written in Golang, which parses the text file and
imports the list into Weaviate. Assumes you have Weaviate running locally on
localhost:8080.

## Status of the features

The features demo'd here are currently (July 2019) in an experimental state and
not officially released yet, so you need to build weaviate yourself to try them
out (for now). Once they are officially released you can just `docker-compose
up -d` weaviate.  
