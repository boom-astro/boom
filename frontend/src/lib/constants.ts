export const PAPER_URL = "https://arxiv.org/abs/2511.00164";
export const ACKNOWLEDGMENTS_TEXT =
  "The Babamul alerts broker and BOOM software infrastructure (du Laz et al. 2026) " +
  "is co-developed by the California Institute of Technology and the University of Minnesota. " +
  "This work acknowledges support from the National Science Foundation through " +
  "AST Award No. 2432476 (PI Kasliwal; co-PI Coughlin) and leverages experience " +
  "from the Zwicky Transient Facility (co-PIs Graham and Kasliwal).";
// Surveys Babamul carries alerts for. The slugs are the ones the API uses in
// paths and the ones that appear in Kafka topic names, so they are the same
// string end to end rather than a display label.
export const ZTF = "ztf";
export const LSST = "lsst";

export const SURVEYS = [LSST, ZTF] as const;
export type Survey = (typeof SURVEYS)[number];

// Every Babamul Kafka topic, named `babamul.{survey}.{cross-match}.{class}`.
// Listed rather than generated from the parts: the combinations aren't a full
// product — only LSST has an `unknown` class.
export const KAFKA_TOPICS = [
  "babamul.ztf.no-lsst-match.stellar",
  "babamul.ztf.lsst-match.stellar",
  "babamul.ztf.no-lsst-match.hosted",
  "babamul.ztf.lsst-match.hosted",
  "babamul.ztf.no-lsst-match.hostless",
  "babamul.ztf.lsst-match.hostless",
  "babamul.lsst.no-ztf-match.stellar",
  "babamul.lsst.ztf-match.stellar",
  "babamul.lsst.no-ztf-match.hosted",
  "babamul.lsst.ztf-match.hosted",
  "babamul.lsst.no-ztf-match.hostless",
  "babamul.lsst.ztf-match.hostless",
  "babamul.lsst.no-ztf-match.unknown",
  "babamul.lsst.ztf-match.unknown",
] as string[];
