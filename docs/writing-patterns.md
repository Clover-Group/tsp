# Writing patterns

(Work in progress)

## Pattern language syntax

For patterns, TSP uses a special (domain-specific) language which
specifies various conditions.

#### Literals

Integers, floating-point numbers and strings are written as usual
(strings are in single quotes), for example:
```
123.456
7890
'this is a string'
```

#### Time literals
They consist of a number (either integer or floating-point) and unit
(full or abbreviated). Examples (and all supported units):
```
2 hours
1.5 hr (same as 90 minutes)
10 minutes
3 min
0.75 seconds (same as 750 ms)
20 sec
568 milliseconds
100 ms
```

(_Note that it's possible to specify a fractional number of milliseconds,
 but it will be still rounded down to the nearest integer._)

Time literals with different units can be combined, for example:
```
2 hours 20 minutes 16 seconds 358 ms  (same as 8416358 ms, etc.)
```

#### Database (source) columns

They are written simply "as-is" if they are identifiers (thus beginning
with a letter, and containing only alpha-numeric characters).
For example:
```
ThisIsAnIdentifier1234
```
If they are not identifiers (contain spaces, for example), they can
be escaped by double-quoting, like this:
```
"this is an identifier"
```

#### Operators

The DSL supports following operators:
- arithmetic: `+`, `-`, `*`, `/`, for example:
    ````
    1.5 * a + (b - c) / 2.0
    ````
- comparison: `<`, `<=`, `>`, `>=`, `=`, `!=`, `<>` (the last 2 both
represent inequality), for example:
```
value < 200
position >= 0
```
- logical: `and`, `or`, `xor`, `not`, for example:
```
Speed >= 0 and (Voltage > 3400 or Voltage < 2700)
```

In all cases, parentheses can be used to override the default priority.

#### Conditions

_Simple condition_ consists only of a boolean expression, such as:
```
Column1 > 0 and Column2 = 1
```
This means "the condition above holds once".

_Timed condition_ consists of a boolean expression, `for` keyword [^1]

and a time literal, for example:
```
Column1 > 0 for 10 minutes
```
This rule requires that the boolean condition (`Column1 > 0`) must
hold for 10 (consecutive) minutes.

_Timed condition with constraint_ has an additional constraint for
total time or the number of times held. It consists of a timed
condition and a constraint of one of the following forms (`<unit>`
can be either a time unit or keyword `times`):
- `<comparison operator> <number> <unit>`, for example:
    ```
    < 30 seconds (less than thirty seconds)
    >= 5 times (at least five times)
    = 1 min (exactly a minute)
    etc.
    ```
- `<number> <unit>? to <number> <unit>` to denote a range, for example:
    ```
    10 sec to 1 min (not less than ten seconds, but at most a minute)
    10 to 60 sec (same; the first <unit> can be omitted)
    1 to 2 times (only once or twice)
    ```

Examples of patterns:
```
Column1 > 0 for 30 sec >= 5 times (at least five times during the thirty-second window)
Column2 = 0 for 1 min < 5 seconds (less than five seconds during the minute window)
```

After a `for` keyword, an optional `exactly` keyword is possible,
denoting that TSP should wait for the exact specified time window, even
if the result becomes clear before the end of it (for example,
after the 5th occurrence of a pattern it's clear that a `>= 5 times`
condition for that pattern is true).

_Combined pattern_ is a combination of the conditions (simple or timed)
via the operators `and`, `or`, and `andThen` (the latter denotes
sequential checking (1st _and then_ 2nd), when the patterns are timed).

Examples:
```
Column1 = 0 for 20 min andThen Column1 > 0 for 3 sec
```

#### Functions
TODO

[^1]: all keywords are case insensitive