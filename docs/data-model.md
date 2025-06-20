# Data model

This page serves as a bit of a glossary for the important entities
handled by the application and how they relate to each other.

## Survey

A survey is some kind of instrument that acquires observations,
e.g., a telescope.

## Object

The most important entity is an object.
This represents something out in space that has been observed.
It can be a supernova, a binary star, etc.

## Alert

An alert is sent by a survey in response to some condition, e.g.,
a transient in brightness,
which may indicate an object has been detected.

An alert always includes an object ID.

Alerts from different surveys will have different schemas,
but they have a few common properties:

- Object ID
- Right ascension (RA)
- Declination (dec)

## Catalog

A catalog is a dataset from a survey.
In BOOM, a survey like ZTF will have multiple catalogs,
one for its alerts, one for auxiliary fields, and one for its image data.

## Cross-match

Objects have IDs defined for a given survey.
A cross-match relates an object's ID in one survey to its ID in another.

## Filter

Users declare filters to define what alerts they would like BOOM to pass
through to an output stream,
since different users are interested in different types of objects.
Since surveys can produce millions of alerts per night,
having filters in place is critical to reduce the effort of detecting
scientifically relevant objects.

### Filter permissions

TODO

## Candidate

TODO

## Program

TODO

## Group

TODO
