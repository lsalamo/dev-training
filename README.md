# Python for Analysts

## Agenda
* 8:30 - 9:05 - Setup Support - We strongly encourage you to complete pre-requisites before the class. But in case you still have some problems we'll be in the room to help you
* 9:05 - Workshop starts!
* 11:00 - Coffee Break
* 13:00 - Lunch
* 16:00 - Workshop finishes


## What you will learn
* How to access Pulse data via Athena
* How to clean, transform and merge data using Python *pandas* library
* How to easily visualize data using *matplotlib*
* How to use Jupyter Notebook efficiently
* How to get insight on your site and provide decision makers with data about the behaviour of users


## What you will NOT learn:
* This is not an introductory programming course. You’re expected to understand basic concepts such as functions, loops or data structures. If you have never used Python you have still plenty of time to get the basics down! https://docs.python.org/3/tutorial/index.html is a good place to start


## Requirements

**Don’t leave it for the last minute, some requests take time to be approved or other issues might occur!**

* You need your own laptop.
* Download and install Anaconda with Python 3.7 for your operating system
* If you already have Python installed you might want to create a clean virtual environment to avoid [Dependency Hell](https://tech.instacart.com/freezing-pythons-dependency-hell-in-2018-f1076d625241) . There are many great tools to manage virtual environments, for example `pyenv` with `virtualwrapper`. Anaconda also offers `conda` tool to manage virtal envs.
* Make sure your PATH environmental variable isn’t messed up. When you type `python` in your command-line you should enter Python interpreter showing Python 3.7.X
* Run `pip install pyathena` in your command line to install Python Athena driver
* Make sure you can use Jupyter Notebook. Running `jupyter notebook` should open it in your default browser.
* Install and configure Heimdall. If you don’t have it in your OKTA you will need to request it. Once added, follow [this instruction](https://confluence.schibsted.io/display/ACCT/Heimdall+-+VPN+as+a+Service+-+User+guide) to configure it.
* Request access to data via Pulse Monitor:
 * Follow these [instructions](https://confluence.schibsted.io/display/SPTTRAC/How+to+get+access+to+data#Howtogetaccesstodata-WhatdoIhaveaccessto) to request access to:
   * `arn:aws:s3:::schibsted-spt-common-prod/yellow/pulse-simple/version=1-alpha/*/client=${provider}` for your site **and** yapocl (put *Python for Analysts course* as request rationale; set 30-Nov as the expiry date)
   * `arn:aws:s3:::schibsted-spt-common-prod/yellow/insights/leads/*/client=${provider}` for your site **and** yapocl (put *Python for Analysts course* as request rationale; set 30-Nov as the expiry date)
 * Request data keys in Pulse Monitor > My account > Databox . Save them in safe place. Use `Sync my permissions` button.
 * Run TEST SETUP notebook to see if everything is OK


If you can't create Databox keys or request dataset please open a ticket similar to [this one](https://jira.schibsted.io/browse/TDA-1084) . 

## Slack

Join **#python-for-analysts** Slack channel ! If you have any doubts or problems with pre-requisites it’s the right place to go.