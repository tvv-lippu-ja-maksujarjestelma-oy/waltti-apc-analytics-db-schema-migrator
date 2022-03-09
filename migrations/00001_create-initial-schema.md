# Create initial schema

## Use surrogate keys

As natural keys in the domain of public transport are deceptively difficult to get right due to decades of worst practices in data architecture, use surrogate keys consistently on all tables.

All surrogate key column names in the schema `apc_gtfs` start with the prefix `unique_`.

## Time zones

Model time zones explicitly.
Use a name like `Europe/Helsinki` instead of an offset.
Combine a `timestamptz` value with a time zone to get a local time value.

Add time zone as a property of stops.

### Reasoning

Finding the right place for the time zone in the data model seems hard.

The simplest solution is to model time zone as a property of a feed publisher.
That would work for us right now.
Some GTFS feed publishers operate on several time zones, though.

A more flexible way is to tie a time zone to a location at a certain time.
In that sense it would make sense to use time zone as a property of stops.
The time zone could be chosen based on e.g. the creation time of the stop.

We could simplify and make the assumption that a new stop will be created if the time zone changes.
That could be painful when time zones change but it happens rarely enough.
The solution would work for agencies that operate across time zones, though.
Let's choose that.

The `start_date` and `start_time` in GTFS Realtime vehicle position messages could be matched to the first available stop of the trip.
