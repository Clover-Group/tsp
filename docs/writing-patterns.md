# Writing patterns

(Work in progress)

## Pattern language syntax

For patterns, TSP uses a special (domain-specific) language which
specifies various conditions.

### Literals

Integers, floating-point numbers and strings are written as usual
(strings are in single quotes), for example:
```
123.456
7890
'this is a string'
```

### Time literals
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

### Database (source) columns

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

### Operators

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

- the `andThen` operator &#8212; holds if the right-hand side holds
immediately after the left-hand side. _Note:the success interval of
`andThen` is the union of the LHS and RHS (i.e. from the timestamp
when LHS starts to the timestamp when RHS ends)._

### Conditions

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

_Note: the success interval of the timed condition starts **after** 
the window ends (i.e. when it becomes known that the condition holds.
E.g. in the pattern `A > 0 for 2 hours`, if A holds from 9:00 to 12:00
hours (during the same day), the reported interval of success will be
from 11:00 to 12:00. See also `wait`)_

_Combined pattern_ is a combination of the conditions (simple or timed)
via the operators `and`, `or`, and `andThen`.

Examples:
```
Column1 = 0 for 20 min andThen Column1 > 0 for 3 sec
```



### Functions
#### Mathematical functions
- Trigonometric functions &#8212; `sin(x)`, `cos(x)`, `tan(x)`/`tg(x)`,
 `cot(x)`/`ctg(x)` &#8212; return the value of the corresponding function
 applied to the argument (expressed in radians)
- Functions with the same names but with `d` suffix &#8212; e.g.
 `sind(x)` &#8212; are applied to the argument in degrees.
- `exp(x)` returns the exponential function _e<sup>x</sup>_.
- `ln(x)` returns the natural logarithm of `x`.
- `log(a, x)` returns the base-`a` logarithm of `x`
(i.e. _log<sub>a</sub>x_).
- `abs(x)` returns the absolute value of `x`.
- `sigmoid(x, a)` returns the Fermi-Dirac sigmoid function, namely
`1 / (1 + exp(-2 * x * a))`.
#### Window functions (aggregating by time)
- `avg(x, time)` returns the value of `x` averaged over the period of
`time`
- `lag(x)` returns the previous value of `x`
- `lag(x, time)` returns the value of `x` which was actual `time` ago
- `wait(time, x)` shifts the start of a success for `x` 
to the left (earlier) for `time` (which is equivalent to actual waiting.
Especially useful for `andThen` conditions)
#### Aggregating functions (by column)
- `avgOf(x1, ..., xn, cond)` compute the average value of `xi` which
satisfy the `cond` condition (NaN's are discarded anyway).
Conditions are written using single underscore (`_`) as a meta-variable.
E.g. `avgOf(x1, x2, x3, _ > 0)` will return `(x1 + x2 + x3) / 3` if
all of `x1`, `x2`, `x3` are positive, or `(x1 + x2) / 2` if only
`x1` and `x2` are, etc.
- `sumOf`, `countOf`, `minOf`, `maxOf` work analogously


[^1]: all keywords are case insensitive