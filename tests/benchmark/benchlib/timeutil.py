"""Time conversions. BOOM stamps documents in JD; Kafka and the replay use Unix time."""

UNIX_EPOCH_JD = 2440587.5
MJD_OFFSET = 2400000.5


def jd_to_unix(jd):
    return (jd - UNIX_EPOCH_JD) * 86400.0


def unix_to_jd(t):
    return t / 86400.0 + UNIX_EPOCH_JD


def mjd_to_jd(mjd):
    return mjd + MJD_OFFSET
