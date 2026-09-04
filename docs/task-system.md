# Task system

Operating a BOOM system typically involves adding new catalogs,
changing the schema of alerts and object already in the database,
and reprocessing alerts already saved in the database, e.g., when a new
catalog or enrichment step, e.g., ML model classifier, is added.
It is important for us to be able to track what mutations were done to the
data when.

BOOM's task system allows kicking off, monitoring, and querying the history
of these tasks from the admin section of the front end.
All tasks report what they've done to mutate the data system, and this
changelog can be viewed from the admin page.
