<h1 align="center">OomStore</h1>
<p align="center">
    <em>Basic, Barebones Feature store using SQLAlchemy</em>
</p>

Status: _design phase_

Goals:

*  Enable a feature store experience without write access using `sqlalchemy`
*  Define features in YAML

# Usage

TBC - this is proposed CLI design

```console
$ spellbook get meta entity --metadata metadata.yml
$ spellbook get meta feature --metadata metadata.yml
$ spellbook get meta group --metadata metadata.yml
$ spellbook join --feature <list of features> --input-file <(optional) labels.csv> --start-date <(optional)> --end-date <(optional)> --output <(optional)>
$ spellbook export --feature <list of features> --latest <(optional)> --start-date --end-date
```

Convenience utilities - this is a wrapper around `pandas` to write to the underlying database, but not needed. It is provided so that the user never needs to leave CLI.

```console
$ spellbook import --group <feature group> --input-file <input file.csv>
```

Python API: TBC, should mirror CLI usage

# Architecture

Well...the difference between Spellbook with other approaches is there is no data created or stored on disc unless you explicitly tell it to write to disc. We don't create a database containing metadata or keep track of revisions. We assume that is handled elsewhere.

# Why not Feast?

Feast is great, you probably should use Feast rather than Spellbook. This project exists more as an exercise for me, as Feast often requires write access to databases, which may not be available to teams depending on organisational structures. 

Spellbook hopefully provides data scientists a "lightweight feature store" experience in constrained environments.

# Inspirations

Full credit needs to be given to the following projects:

*  Feast - https://github.com/oom-ai/oomstore
*  Oomstore - https://github.com/oom-ai/oomstore
