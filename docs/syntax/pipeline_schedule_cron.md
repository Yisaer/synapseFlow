# Pipeline Schedule Cron Syntax

Pipeline schedules accept Linux-compatible five-field cron expressions evaluated in UTC:

```text
minute hour day-of-month month day-of-week
```

The supported field values are:

| Field | Values |
| --- | --- |
| Minute | `0-59` |
| Hour | `0-23` |
| Day of month | `1-31` |
| Month | `1-12` or `JAN-DEC` |
| Day of week | `0-7` or `SUN-SAT`; both `0` and `7` mean Sunday |

Fields support wildcards, lists, ranges, and steps. For example,
`*/10 8-17 * * MON-FRI` fires every ten minutes during UTC business hours from Monday through
Friday.

When both day-of-month and day-of-week are restricted, a timestamp matches when either field
matches, following Linux cron semantics. For example, `30 4 1,15 * FRI` fires at 04:30 UTC on the
first and fifteenth day of every month and on every Friday.

The recurring nicknames `@yearly`, `@annually`, `@monthly`, `@weekly`, `@daily`, `@midnight`, and
`@hourly` are supported. `@reboot` is not supported because pipeline schedules describe recurring
run windows rather than process-start events.
