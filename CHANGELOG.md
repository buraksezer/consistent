# Changelog

## 1.0.0

### Fixed

- `averageLoad` used integer division, so the average was wrong when the
  partition count was not a multiple of the member count. It is now computed
  with floating-point math. (#16)
- Replica keys were built as `name + index`, so different members could produce
  the same ring key (for example `node1` + `1` and `node11` + `0`). Keys are now
  built as `index:name`. (#17)
- `distributeWithLoad` could panic before checking the last position on the
  ring. The loop now visits every position. (#13)
- `GetClosestNForPartition` panicked on a partition id that is out of range. It
  returns the new `ErrPartitionNotFound` error instead.
- `New` no longer skips partition distribution when it gets an empty, non-nil
  member slice.

### Changed

- The internal maps store `Member` values instead of `*Member`. This removes a
  copy on every lookup. Public API is unchanged.
- The minimum Go version is now 1.22. CI runs on Go 1.25.x and 1.26.x.

### Added

- `ErrPartitionNotFound`.
- Benchmarks for the main operations.
- More tests: edge cases, concurrency, and regression tests for the fixes above.
  Tests are run with the race detector.

### Removed

- Travis CI configuration.
