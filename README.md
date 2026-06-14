# trafiklab-pipeline

## Data Quality Notes

Delays measured at `timepoint=0` stops are less reliable because the schedule there is itself an estimate. A polished analysis should only trust delays at `timepoint=1` stops, where the schedule time is an actual timepoint commitment.
