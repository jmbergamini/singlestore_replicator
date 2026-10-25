Reference run fixture for the calibrate test.
Author: John Bergamini

copy-journal.sqlite is the journal of the reference copy run (95 tables, 382 chunks, 4 workers,
S-16 compute at 2x), with every table renamed t001 to t095 (largest first), every column renamed
c01, c02 and so on, and the database names replaced by reference_src and reference_rebuild. The
chunk timings, row counts and bucket counts are the originals. The fit in estimate.calibrate on
the first-pass chunks that ended before 08:00Z reproduces the shipped baseline constants.
Primary files of the source measured 938 GiB in total; the journal holds no per-table sizes.
