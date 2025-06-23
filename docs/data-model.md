# Data model

This page serves as a bit of a glossary for the important entities
handled by the application and how they relate to each other.

## Survey

A survey is an astronomical observing program or campaign that systematically
collects observations over large areas of the sky using one or more telescopes.
These are not only equipped with cutting-edge wide field-of-view instruments
that allow them to survey our night,
but have large scale data processing capabilities to provide the astronomical
community with a continuous real-time stream of "alerts".
Examples include the Zwicky Transient Facility (ZTF) or the Vera C. Rubin
Observatory's Legacy Survey of Space and Time (LSST).

## Object

The most important entity is an object (also called a "source").
An object represents a distinct astrophysical entity that has been observed,
such as a star, galaxy, quasar, supernova, asteroid, or other celestial body.
Since objects are independently detected by each survey,
they are assigned their own distinct object ID by each survey,
based on position---when a change in brightness is detected for the first time
at a given location, a unique `objectId` is assigned to it.
Thereafter, measurements at the same location
(within some positional uncertainty)
are associated to the same `objectId`.

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
