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
Effectively, this indicates us that a new transient has been discovered (if nothing was present at that location in the reference image) or a known star is varying in brightness.
Since objects are independently detected by each survey,
they are assigned their own distinct object ID by each survey,
based on position---when a change in brightness is detected for the first time
at a given location, a unique `objectId` is assigned to it.
Thereafter, measurements at the same location
(within some positional uncertainty)
are associated to the same `objectId`.

## Alert

An alert, also known as a "candidate,"
is a message sent by a survey notifying any subscribers of a change
in brightness of a known object or the detection of a new one.
Most astronomical surveys rely on a system of "reference" images,
which are stacks of multiple images of the same location in the sky.
When new images are acquired,
a subtraction/difference between the new images and the reference image is
computed.
As a result, a pipeline run by the survey
can detect differences in brightness anywhere on the
subtraction/difference image,
and "alert" us when a significant enough difference is measured.

Essentially, each alert indicates to us that a new transient has been
discovered (if nothing was present at that location in the reference image) or
a known star is varying in brightness.

An alert always includes an object ID, along with a candidate ID,
which serves as the unique identifier for that alert.

Alerts from different surveys will have different schemas,
but they have a few common properties:

- Object ID
- Candidate ID
- Right ascension (RA)
- Declination (dec)
- Brightness and associated error (in magnitude and/or flux-space)
- New, reference,
  and difference images (angular and pixel size varying per survey)
- Various quality flags and metadata about the associated alert

## Catalog

A catalog is a dataset from a survey.
Internally, a catalog maps one-to-one with a MongoDB collection.
BOOM stores both _archival_ and _live_ catalogs,
the former being static sets of objects detected by a survey.
Archival catalogs will only contain objects,
but live catalogs will come in sets of three with different data types:

- Alerts (typically named like `{survey_name}_alerts`,
  with candidate ID as the unique identifier)
- Objects (typically named like `{survey_name}_alerts_aux`)
- Images (typically named like `{survey_name}_alerts_cutouts`)

## Alert input stream

A live survey has one or more alert input streams to which BOOM can subscribe.
Each alert stream contains a different
subset of alerts and has its own access restrictions.
For example,
ZTF has a public stream, a partnership stream, and a Caltech-only stream.

## Cross-match

A cross-match relates an object's ID in one survey to its ID in another
based on location.
It's possible that two distinct objects could be detected at the same location,
due to positional uncertainty, distance, and resolution.
BOOM cross-matches object IDs in live catalogs with archival catalogs
the first time an object shows up in an alert, and only that first time.

## Filter

Users declare filters to define what alerts they would like BOOM to pass
through to an output stream,
since different users are interested in different types of objects.
Since surveys can produce millions of alerts per night,
having filters in place is critical to reduce the effort of detecting
scientifically relevant objects.

Filters can make use of the cross-matching information since BOOM performs
cross-matching before applying filters.
Filters can also make use of light curve data from other live surveys,
which is as far as we know not possible with any other alert broker.

Each filter is associated with one alert input stream
and one alert output stream.
Each alert output stream has its own credentials for authorization.
These can be used, for example, to connect a Marshal like SkyPortal.
