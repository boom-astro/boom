# Science validation

These scripts measure whether the context BOOM attaches to alerts is correct,
not how fast it is attached. They run offline against a database snapshot (and,
for similarity search, the API of the same deployment). None of them start any
services.

| Script | Question it answers |
|---|---|
| `cross_survey_matching.py` | For each pair of surveys: how often objects are linked, how far apart linked objects are, and how often a match is chance (from lookups at shifted positions). |
| `host_vs_tns.py` | For transients with a TNS redshift: how often a host is found, how well the host's redshift agrees, by catalog. Also compares DLR ranking against simply taking the nearest candidate. |
| `similarity_purity.py` | Whether an object's nearest neighbors in AppleCiDEr embedding space share its TNS class, compared with that class's base rate. |

`host_vs_tns.py` cannot report the spectroscopic share of host redshifts
directly. A stored host records `z` and `z_err`, but not where the redshift
came from. The per-catalog breakdown is the closest available proxy: NED-LVS
and DESI rows are mostly spectroscopic, Legacy Surveys rows are photometric.

## Not yet scripted: GW injection–recovery

The main multi-messenger claim is that the 3D credible-volume cut removes
unrelated candidates while keeping real counterparts. Testing it needs:

1. Simulated BNS/NSBH skymaps with known true positions and distances, for
   example from the LVK observing-scenarios simulations.
2. Truth transients that have a host and a redshift, e.g. the TNS-classified
   objects used by `host_vs_tns.py`. Pair each truth transient with a skymap
   whose true distance matches the transient's redshift. Rotate the skymap so
   its true position lands on the transient.
3. Run both `query/skymap.py` variants (with and without distance layers) over
   the replayed night, with the truth transients included.
4. Report recovery (the fraction of truth transients returned) and
   contamination (all other candidates returned) at 90% and 95% for 2D and 3D.
   This gives the headline number for the GW section: "the volume cut removes
   X% of candidates at Y% efficiency".

Step 2 needs care. Rotating a skymap moves its probability along with the true
position, so the rotated map is still a valid localization. Drawing positions
from an unrotated map instead would put truth transients where no real galaxy
is.
