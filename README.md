Watch and Fork events for popular repos
===================================================

`popular_repos_1000.csv` -- repositories that have throusands stars and forks in 2015

`active_users.csv` -- users that starred/forked 10..200 repositories since 2014-01-01

Usage
-----

Put your json files in `files` dir and run

`json_reader.py files`

It will read through them and create csv files in `files` containing only lines
relating only to Watch and Fork events by active users on popular repositories.

Why?
----

For some data mining :)
